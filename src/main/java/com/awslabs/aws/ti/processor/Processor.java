package com.awslabs.aws.ti.processor;

import com.awslabs.aws.ti.Record;

/**
 * Transformation Instance Processor interface. These are intermediary processing units using which users can filter,
 * transform and enrich the records into desired format before publishing to the sink.
 */
public interface Processor {

    /**
     * execute the processor logic which could potentially modify the incoming record. The level to which the record has
     * been modified depends on the implementation
     *
     * @param record Record which will be modified
     * @return Record modified record
     */
    Record execute(final Record record);
}
