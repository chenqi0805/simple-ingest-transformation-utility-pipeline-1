package com.amazon.ti.model.source;

import com.amazon.ti.model.record.Record;
import com.amazon.ti.model.buffer.Buffer;

/**
 * Transformation Instance source interface. Source acts as receiver of the events that flow
 * through the transformation pipeline.
 */
public interface Source<T extends Record<?>> {

    /**
     * Notifies the source to start writing the records into the buffer
     *
     * @param buffer Buffer to which the records will be queued or written to.
     */
    void start(Buffer<T> buffer);

    /**
     * Notifies the source to stop consuming/writing records into Buffer.
     */
    void stop();
}
