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
    private static final ThreadLocal<Deflater> DEFLATER = ThreadLocal.withInitial(() -> new Deflater(Deflater.DEFAULT_COMPRESSION, true));

    private final Path filePath;
    private final String nullHandling;
    private final int batchSize;
    private final int writerBufferBytes;
    private final List<Object> recordBuffer;
    private FileOutputStream fileOut;
    private boolean closed = false;

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
        if (closed) {
            throw new IOException("Writer is closed");
        }

        if (record.value() == null && !"write".equals(nullHandling)) {
            return;
        }

        Object value = record.value();
        String json = value == null ? "null" : value.toString();
        recordBuffer.add(json);

        if (recordBuffer.size() >= batchSize) {
            flushBuffer();
        }
    }

    @Override
    public void flush() throws IOException {
        flushBuffer();
    }

    private void flushBuffer() throws IOException {
        if (recordBuffer.isEmpty() || fileOut == null) {
            return;
        }

        LOGGER.debug("Flushing {} records to {}", recordBuffer.size(), filePath);

        Deflater deflater = DEFLATER.get();
        deflater.reset();

        try (BufferedOutputStream bufferedOut = new BufferedOutputStream(fileOut, Math.max(8 * 1024, writerBufferBytes));
             CustomGZIPOutputStream gzipOut = new CustomGZIPOutputStream(bufferedOut, deflater);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(gzipOut, StandardCharsets.UTF_8), Math.max(8 * 1024, writerBufferBytes))) {

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
            gzipOut.finish();
        }

        recordBuffer.clear();
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
        if (!closed) {
            try {
                flushBuffer(); // Ensure any remaining records are written
            }
            finally {
                closed = true;
                if (fileOut != null) {
                    fileOut.close();
                    fileOut = null;
                }
                Deflater deflater = DEFLATER.get();
                if (deflater != null) {
                    deflater.end();
                    DEFLATER.remove();
                }
            }
        }
    }
}
