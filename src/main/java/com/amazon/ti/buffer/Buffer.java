package com.amazon.ti.buffer;

import com.amazon.ti.Record;
import com.amazon.ti.plugins.buffer.UnboundedInMemoryBuffer;

import java.util.Collection;

/**
 * Transformation Instance buffer interface. Buffer queues the records between TI components and acts as a layer
 * between source and processor/sink. Buffer can be in-memory, disk based or other a standalone implementation.
 *
 * TODO: Rename this such that it does not confuse java.nio.Buffer
 */
public interface Buffer<T extends Record<?>> {

    /**
     * writes the record to the buffer
     * @param record The Record which needed to be written
     */
    void put(T record);

    /**
     * @return The earliest record in the buffer which is still not read.
     */
    T get();

    /**
     * @return Collection of records from the buffer
     */
    Collection<T> records();

    @SuppressWarnings("rawtypes")
     static Buffer defaultBuffer() {
        return new UnboundedInMemoryBuffer();
    }
}
