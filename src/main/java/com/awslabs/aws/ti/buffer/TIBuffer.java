package com.awslabs.aws.ti.buffer;

import com.awslabs.aws.ti.Record;

/**
 * Transformation Instance buffer interface. Buffer queues the records between TI components and acts as a layer
 * between source and processor/sink. Buffer can be in-memory, disk based or other a standalone implementation.
 */
public interface TIBuffer {

    /**
     * writes the record to the buffer
     * @param record The Record which needed to be written
     */
    void put(Record record);

    /**
     * @return The earliest record in the buffer which is still not read.
     */
    Record get();
}
