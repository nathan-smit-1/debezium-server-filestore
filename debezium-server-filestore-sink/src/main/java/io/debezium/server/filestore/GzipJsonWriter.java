/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.filestore;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.Deflater;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;

/**
 * Implementation of RecordWriter that writes records as GZIP compressed JSON lines.
 */
public class GzipJsonWriter implements RecordWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(GzipJsonWriter.class);

    private final Path filePath;
    private final String nullHandling;
    private final int batchSize;
    private final int writerBufferBytes;
    private final List<Object> recordBuffer;
    private final Object lock = new Object();
    private FileOutputStream fileOut;
    private Deflater deflater;
    private volatile boolean closed = false;
    
    // Track flush mode to avoid log spam
    private String currentFlushMode = "normal";
    private long lastModeTransitionTime = 0;
    private long lastFlushStartTime = 0;
    private long lastFlushDuration = 0;
    private final long SLOW_FLUSH_THRESHOLD_MS = 1000; // Consider flush slow if > 1 second

    public GzipJsonWriter(Path filePath, String nullHandling, int batchSize) {
        this.filePath = filePath;
        this.nullHandling = nullHandling;
        this.batchSize = batchSize;
        this.writerBufferBytes = 256 * 1024; // default 256 KiB
        this.recordBuffer = new java.util.ArrayList<>(batchSize);
    }

    public GzipJsonWriter(Path filePath, String nullHandling, int batchSize, int writerBufferBytes) {
        this.filePath = filePath;
        this.nullHandling = nullHandling;
        this.batchSize = batchSize;
        this.writerBufferBytes = Math.max(8 * 1024, writerBufferBytes); // sanity lower bound
        this.recordBuffer = new java.util.ArrayList<>(batchSize);
    }

        @Override
    public void writeRecord(ChangeEvent<Object, Object> record) throws IOException {
        synchronized (lock) {
            if (closed) {
                throw new IOException("Writer is closed");
            }

            if (record.value() == null && !"write".equals(nullHandling)) {
                return;
            }

            Object value = record.value();
            String json = value == null ? "null" : value.toString();
            recordBuffer.add(json);

            // Check memory pressure and flush aggressively if needed
            Runtime runtime = Runtime.getRuntime();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            long usedMemory = totalMemory - freeMemory;
            double memoryUsagePercent = (double) usedMemory / totalMemory * 100;

            // Determine optimal flush strategy based on memory pressure and I/O performance
            boolean shouldFlush = false;
            String newFlushMode;
            boolean isSlowIO = lastFlushDuration > SLOW_FLUSH_THRESHOLD_MS;
            
            if (memoryUsagePercent > 95) {
                // Emergency - always flush immediately regardless of I/O performance
                shouldFlush = recordBuffer.size() > 0;
                newFlushMode = "emergency";
            } else if (memoryUsagePercent > 90) {
                if (isSlowIO) {
                    // If I/O is slow and memory is high, try larger batches to improve throughput
                    shouldFlush = recordBuffer.size() >= Math.max(batchSize, batchSize * 2);
                    newFlushMode = "aggressive-large";
                } else {
                    // I/O is fast, use smaller batches to reduce memory
                    shouldFlush = recordBuffer.size() >= Math.max(1, batchSize / 4);
                    newFlushMode = "aggressive-small";
                }
            } else if (memoryUsagePercent > 80) {
                if (isSlowIO) {
                    // Use normal batch size when I/O is slow to balance memory and throughput
                    shouldFlush = recordBuffer.size() >= batchSize;
                    newFlushMode = "early-normal";
                } else {
                    // I/O is fast, use smaller batches
                    shouldFlush = recordBuffer.size() >= Math.max(1, batchSize / 2);
                    newFlushMode = "early-small";
                }
            } else {
                // Normal flushing
                shouldFlush = recordBuffer.size() >= batchSize;
                newFlushMode = "normal";
            }

            // Log only for emergency mode and when returning to normal
            if (!newFlushMode.equals(currentFlushMode)) {
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastModeTransitionTime > 5000) { // Throttle mode change logs to once per 5 seconds
                    if ("normal".equals(newFlushMode)) {
                        LOGGER.debug("Memory pressure relieved ({}%), returning to normal batch flushing (batch size: {})", 
                                   Math.round(memoryUsagePercent), batchSize);
                        currentFlushMode = newFlushMode;
                        lastModeTransitionTime = currentTime;
                    } else if ("emergency".equals(newFlushMode)) {
                        LOGGER.warn("Critical memory pressure detected ({}%), switching to emergency flushing mode (any records)", 
                                   Math.round(memoryUsagePercent));
                        currentFlushMode = newFlushMode;
                        lastModeTransitionTime = currentTime;
                    } else if (newFlushMode.contains("large") && !currentFlushMode.contains("large")) {
                        LOGGER.debug("I/O performance optimization: switching to larger batches (2x) due to slow I/O (last flush: {}ms)", 
                                   lastFlushDuration);
                        currentFlushMode = newFlushMode;
                        lastModeTransitionTime = currentTime;
                    } else {
                        // For other modes, just update tracking without logging
                        currentFlushMode = newFlushMode;
                    }
                }
            }

            if (shouldFlush) {
                flushBuffer();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        synchronized (lock) {
            flushBuffer();
        }
    }

    private void flushBuffer() throws IOException {
        if (recordBuffer.isEmpty() || fileOut == null || closed) {
            return;
        }

        LOGGER.debug("Flushing {} records to {}", recordBuffer.size(), filePath);
        
        // Track flush timing for adaptive batching
        lastFlushStartTime = System.currentTimeMillis();

        // Ensure deflater is initialized
        if (deflater == null) {
            deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
        }
        
        try {
            deflater.reset();
        } catch (Exception e) {
            LOGGER.warn("Deflater was closed, creating new one: {}", e.getMessage());
            deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
            deflater.reset();
        }

        // Manual resource management to avoid closing the underlying fileOut stream
        BufferedOutputStream bufferedOut = null;
        CustomGZIPOutputStream gzipOut = null;
        OutputStreamWriter osw = null;
        BufferedWriter writer = null;
        boolean success = false;
        
        try {
            bufferedOut = new BufferedOutputStream(fileOut, Math.max(8 * 1024, writerBufferBytes));
            gzipOut = new CustomGZIPOutputStream(bufferedOut, deflater);
            osw = new OutputStreamWriter(gzipOut, StandardCharsets.UTF_8);
            writer = new BufferedWriter(osw, Math.max(8 * 1024, writerBufferBytes));

            for (Object record : recordBuffer) {
                if (record instanceof String) {
                    writer.write((String) record);
                }
                else if (record instanceof byte[]) {
                    writer.flush();
                    gzipOut.write((byte[]) record);
                }
                writer.write('\n');
            }
            writer.flush();
            gzipOut.finish(); // Mark as successful only after finish() completes
            success = true;
            
        } finally {
            // Ensure proper GZIP completion to avoid corruption
            if (gzipOut != null && !success) {
                try {
                    // If we didn't succeed, try to finish the GZIP stream anyway to avoid corruption
                    gzipOut.finish();
                } catch (Exception e) {
                    LOGGER.warn("Error finishing GZIP stream after failure: {}", e.getMessage());
                }
            }
            
            // Close streams in reverse order, but don't close the underlying fileOut
            if (writer != null) {
                try {
                    writer.flush();
                } catch (Exception e) {
                    LOGGER.debug("Error flushing writer: {}", e.getMessage());
                }
            }
            if (gzipOut != null) {
                try {
                    gzipOut.flush();
                } catch (Exception e) {
                    LOGGER.debug("Error flushing gzip stream: {}", e.getMessage());
                }
            }
            if (bufferedOut != null) {
                try {
                    bufferedOut.flush();
                } catch (Exception e) {
                    LOGGER.debug("Error flushing buffered stream: {}", e.getMessage());
                }
            }
            // Note: We intentionally do NOT close fileOut here, as it needs to remain open
        }

        recordBuffer.clear();
        
        // Track flush duration for adaptive batching
        lastFlushDuration = System.currentTimeMillis() - lastFlushStartTime;
        
        // Suggest GC if we just freed up memory under pressure
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        double memoryUsagePercent = (double) (totalMemory - freeMemory) / totalMemory * 100;
        
        if (memoryUsagePercent > 90) {
            LOGGER.debug("High memory usage after flush ({}%), suggesting GC", Math.round(memoryUsagePercent));
            System.gc(); // Suggest garbage collection when under memory pressure
        }
    }

    @Override
    public void initialize() throws IOException {
        this.fileOut = new FileOutputStream(filePath.toFile(), true);
    }

    @Override
    public String getFileExtension() {
        return ".json.gz";
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            if (!closed) {
                closed = true; // Set closed first to prevent other operations
                try {
                    flushBuffer(); // Ensure any remaining records are written
                }
                catch (Exception e) {
                    LOGGER.warn("Error flushing buffer during close: {}", e.getMessage());
                }
                finally {
                    if (fileOut != null) {
                        try {
                            fileOut.close();
                        }
                        finally {
                            fileOut = null;
                        }
                    }
                    
                    // Clean up deflater
                    if (deflater != null) {
                        try {
                            deflater.end();
                        }
                        catch (Exception e) {
                            LOGGER.debug("Error ending deflater: {}", e.getMessage());
                        }
                        finally {
                            deflater = null;
                        }
                    }
                }
            }
        }
    }
}
