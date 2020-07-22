package com.awslabs.aws.ti.source;

import com.awslabs.aws.ti.buffer.TIBuffer;

/**
 * Transformation Instance source interface. Source acts as receiver of the events that flow
 * through the transformation pipeline.
 */
public interface Source {

    /**
     * Notifies the source to start writing the records into the buffer
     * @param buffer Buffer to which the records will be queued or written to.
     */
    void start(final TIBuffer buffer);

    /**
     * Notifies the source to stop consuming/writing records into Buffer.
     */
    void stop();
}
