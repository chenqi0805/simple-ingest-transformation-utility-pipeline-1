package com.amazon.ti.processor;

import com.amazon.ti.Record;

/**
 * Transformation Instance Processor interface. These are intermediary processing units using which users can filter,
 * transform and enrich the records into desired format before publishing to the sink.
 */
public interface Processor {

    /**
     * execute the processor logic which could potentially modify the incoming record. The level to which the record has
     * been modified depends on the implementation
     *
     * @param record Input record that will be modified/processed
     * @return Record  modified output record
     */
    Record execute(final Record record);
}
