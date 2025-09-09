/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.filestore;

import java.io.IOException;

import io.debezium.engine.ChangeEvent;

/**
 * Interface for writing records to files in different formats.
 */
public interface RecordWriter extends AutoCloseable {
    /**
     * Write a record to the file
     * @param record The record to write
     * @throws IOException If there is an error writing the record
     */
    void writeRecord(ChangeEvent<Object, Object> record) throws IOException;

    /**
     * Initialize any resources needed for writing
     * @throws IOException If there is an error initializing resources
     */
    void initialize() throws IOException;

    /**
     * Force a flush of any buffered records
     * @throws IOException If there is an error flushing records
     */
    default void flush() throws IOException {
        // Default implementation does nothing
    }

    /**
     * Get the file extension for this writer's format
     * @return The file extension (e.g., ".json.gz" or ".parquet")
     */
    String getFileExtension();
}
