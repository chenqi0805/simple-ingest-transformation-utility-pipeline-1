package com.amazon.ti.source;

import com.amazon.ti.Record;
import com.amazon.ti.buffer.Buffer;

/**
 * Transformation Instance source interface. Source acts as receiver of the events that flow
 * through the transformation pipeline.
 */
public interface Source<InputRecord extends Record<?>> {

    /**
     * Notifies the source to start writing the records into the buffer
     * @param buffer Buffer to which the records will be queued or written to.
     */
    void start(final Buffer<InputRecord> buffer);

    /**
     * Notifies the source to stop consuming/writing records into Buffer.
     */
    void stop();
}
