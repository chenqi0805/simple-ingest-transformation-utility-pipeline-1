package com.amazon.ti.sink;

import com.amazon.ti.Record;
import java.util.Collection;

/**
 * Transformation Instance sink interface. Sink may publish records to a disk or a file or
 * to elasticsearch or other pipelines or external systems
 */
public interface Sink {

    /**
     * outputs collection of {@link Record}.
     * @param records
     */
    boolean output(Collection<Record> records);

    /**
     * Updates the sink to stop sending records.
     */
    void stop();
}
