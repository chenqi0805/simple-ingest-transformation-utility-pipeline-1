package com.awslabs.aws.ti.processor;

import com.awslabs.aws.ti.Record;
import com.awslabs.aws.ti.buffer.Buffer;

import java.util.Collection;

/**
 * Transformation Instance Processor interface. These are intermediary processing units using which users can filter,
 * transform and enrich the records into desired format before publishing to the sink.
 */
public interface Processor {

    Collection<Record> process(final Buffer buffer);
}
