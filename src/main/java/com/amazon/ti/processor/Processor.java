package com.amazon.ti.processor;

import com.amazon.ti.Record;

import java.util.Collection;

/**
 * Transformation Instance Processor interface. These are intermediary processing units using which users can filter,
 * transform and enrich the records into desired format before publishing to the sink.
 */
public interface Processor<InputRecord extends Record<?>, OutputRecord extends Record<?>> {

    /**
     * execute the processor logic which could potentially modify the incoming record. The level to which the record has
     * been modified depends on the implementation
     *
     * @param records Input records that will be modified/processed
     * @return Record  modified output records
     */
    Collection<OutputRecord> execute(final Collection<InputRecord> records);
}
