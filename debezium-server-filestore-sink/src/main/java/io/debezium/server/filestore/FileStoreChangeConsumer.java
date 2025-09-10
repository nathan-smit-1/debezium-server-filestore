/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.filestore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.Header;
import io.debezium.server.BaseChangeConsumer;

/**
 * Implementation of the consumer that writes messages to files based on their destination.
 */
@Named("filestore")
@Dependent
public class FileStoreChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileStoreChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.filestore.";

    private static final java.util.regex.Pattern PAYLOAD_PATTERN = java.util.regex.Pattern.compile("\"payload\"\\s*:\\s*\"([^\"]*)\"");

    @ConfigProperty(name = PROP_PREFIX + "null.handling", defaultValue = "ignore")
    String nullHandling;

    @ConfigProperty(name = PROP_PREFIX + "directory", defaultValue = "data")
    String directory;

    @ConfigProperty(name = PROP_PREFIX + "header.filter.name")
    Optional<String> headerFilterName;

    @ConfigProperty(name = PROP_PREFIX + "header.filter.value")
    Optional<String> headerFilterValue;

    @ConfigProperty(name = PROP_PREFIX + "batch.size", defaultValue = "1000")
    int batchSize;

    @ConfigProperty(name = PROP_PREFIX + "flush.interval.ms", defaultValue = "1000")
    int flushIntervalMs;

    @ConfigProperty(name = PROP_PREFIX + "writer.buffer.bytes", defaultValue = "262144")
    int writerBufferBytes;

    @ConfigProperty(name = PROP_PREFIX + "pass.through.sink")
    Optional<String> passThroughSinkName;

    @Inject
    Instance<DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>> allChangeConsumers;

    private Path baseDir;
    private final Map<String, RecordWriter> writers = new HashMap<>();
    private long lastFlushMillis = System.currentTimeMillis();
    private DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> passThroughConsumer;
    private boolean filterEnabled;

    /**
     * Efficiently extracts the payload value from a header that contains JSON with schema and payload.
     * Uses caching to avoid repeated parsing of the same header value.
     */
    private String extractHeaderValue(Header<?> header) {
        Object rawValue = header.getValue();
        if (rawValue == null) {
            return null;
        }

        if (!(rawValue instanceof String)) {
            return rawValue.toString();
        }

        String strValue = (String) rawValue;
        if (!strValue.startsWith("{\"schema\":")) {
            return strValue;
        }

        java.util.regex.Matcher matcher = PAYLOAD_PATTERN.matcher(strValue);
        if (matcher.find()) {
            return matcher.group(1);
        }

        LOGGER.debug("Failed to extract payload using regex from value: {}", strValue);
        return strValue;
    }

    @PostConstruct
    void connect() {
        try {
            baseDir = Paths.get(directory);
            Files.createDirectories(baseDir);
            boolean configuredFilter = headerFilterName.isPresent() && !headerFilterName.get().isEmpty() &&
                                     headerFilterValue.isPresent() && !headerFilterValue.get().isEmpty();

            // Resolve a pass-through consumer if configured
            if (passThroughSinkName.isPresent() && !passThroughSinkName.get().isEmpty()) {
                Instance<DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>> selected = allChangeConsumers
                        .select(NamedLiteral.of(passThroughSinkName.get()));
                if (selected.isResolvable()) {
                    passThroughConsumer = selected.get();
                    LOGGER.info("Initialized pass-through consumer by name: '{}'", passThroughSinkName.get());
                }
                else {
                    LOGGER.warn("Pass-through sink '{}' not found; pass-through disabled", passThroughSinkName.get());
                }
            }

            // Enable the filter only if it's configured AND a pass-through consumer exists.
            // Otherwise, disable the filter and write all records to files.
            if (configuredFilter && passThroughConsumer == null) {
                LOGGER.warn("Header filter configured (name='{}', value='{}') but no pass-through sink configured; disabling filter and writing all records to files.",
                        headerFilterName.orElse(""), headerFilterValue.orElse(""));
                filterEnabled = false;
            }
            else {
                filterEnabled = configuredFilter;
            }

        LOGGER.info("Using configuration: null.handling='{}', directory='{}', filter.enabled='{}', filter.name='{}', filter.value='{}', pass.through.sink='{}', writer.buffer.bytes={}",
            nullHandling, directory, filterEnabled, headerFilterName.orElse(""), headerFilterValue.orElse(""), passThroughSinkName.orElse(""), writerBufferBytes);
        }
        catch (IOException e) {
            throw new DebeziumException("Unable to create directory " + directory, e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        boolean anyRecordsWritten = false;
        final List<ChangeEvent<Object, Object>> forwarded = new ArrayList<>();

        for (ChangeEvent<Object, Object> record : records) {
            try {
                String destination = record.destination();
                if (destination == null) {
                    LOGGER.warn("Skipping record with null destination");
                    committer.markProcessed(record);
                    continue;
                }

                boolean shouldWriteToDisk;

                if (filterEnabled) {
                    String actualValue = null;
                    if (record.headers() != null) {
                        for (Header<?> header : record.headers()) {
                            if (headerFilterName.isPresent() && headerFilterName.get().equals(header.getKey())) {
                                actualValue = extractHeaderValue(header);
                                break;
                            }
                        }
                    }
                    shouldWriteToDisk = headerFilterValue.isPresent() && headerFilterValue.get().equals(actualValue);
                    LOGGER.debug("Filter check - header: {}, expected: {}, actual value: {}, matches: {}",
                            headerFilterName.orElse(""), headerFilterValue.orElse(""), actualValue, shouldWriteToDisk);
                }
                else {
                    shouldWriteToDisk = true;
                }

                LOGGER.debug("Decision factors - shouldWriteToDisk: {}, hasFilter: {}, record.value() != null: {}, nullHandling: {}",
                        shouldWriteToDisk, filterEnabled, record.value() != null, nullHandling);

                if (shouldWriteToDisk && (record.value() != null || "write".equals(nullHandling))) {
                    LOGGER.debug("Writing record to disk - destination: {}", destination);
                    RecordWriter writer = writers.computeIfAbsent(destination, this::createWriter);
                    writer.writeRecord(record);
                    anyRecordsWritten = true;
                    committer.markProcessed(record);
                }
                else if (filterEnabled && !shouldWriteToDisk) {
                    forwarded.add(record);
                }
                else {
                    committer.markProcessed(record);
                }
            }
            catch (IOException e) {
                throw new DebeziumException("Failed to write record to file", e);
            }
        }

        if (!forwarded.isEmpty()) {
            if (passThroughConsumer != null) {
                // Only flush writers that have received records this batch
                if (anyRecordsWritten) {
                    flushActiveWriters();
                }

                LOGGER.debug("Forwarding {} record(s) to pass-through sink '{}'", forwarded.size(),
                        passThroughSinkName.map(s -> s.isEmpty() ? "<unspecified>" : s).orElse("<unspecified>"));
                passThroughConsumer.handleBatch(forwarded, committer);
                return;
            }
            else {
                LOGGER.warn("No pass-through consumer available; marking {} forwarded record(s) as processed", forwarded.size());
                for (ChangeEvent<Object, Object> record : forwarded) {
                    committer.markProcessed(record);
                }
            }
        }

        long currentTime = System.currentTimeMillis();
        boolean shouldTimeFlush = currentTime - lastFlushMillis >= flushIntervalMs;

        if (shouldTimeFlush) {
            flushActiveWriters();
            lastFlushMillis = currentTime;
        }

        try {
            committer.markBatchFinished();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    @Override
    public boolean supportsTombstoneEvents() {
        return false;
    }

    private RecordWriter createWriter(String destination) {
        try {
            RecordWriter writer = new GzipJsonWriter(baseDir.resolve(destination + ".json.gz"), nullHandling, batchSize, writerBufferBytes);
            writer.initialize();
            return writer;
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to create writer for destination: " + destination, e);
        }
    }

    private void flushActiveWriters() {
        for (RecordWriter writer : writers.values()) {
            try {
                writer.flush();
            }
            catch (IOException e) {
                throw new DebeziumException("Failed to flush records", e);
            }
        }
    }

    @PreDestroy
    void close() {
        for (RecordWriter writer : writers.values()) {
            try {
                writer.close();
            }
            catch (Exception e) {
                LOGGER.error("Error closing writer", e);
            }
        }
    }
}
