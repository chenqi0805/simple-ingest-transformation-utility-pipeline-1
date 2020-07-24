package com.amazon.ti.sink;

import com.amazon.ti.Record;
import java.util.Collection;

/**
 * Transformation Instance sink interface. Sink may publish records to a disk or a file or
 * to elasticsearch or other pipelines or external systems
 */
public interface Sink<OutputRecord extends Record<?>> {

    /**
     * outputs collection of records which extend {@link Record}.
     * @param records
     */
    boolean output(Collection<OutputRecord> records);

    /**
     * Updates the sink to stop sending records.
     */
    void stop();
}
