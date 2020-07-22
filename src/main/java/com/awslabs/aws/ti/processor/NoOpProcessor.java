package com.awslabs.aws.ti.processor;

import com.awslabs.aws.ti.Record;

public class NoOpProcessor implements Processor {
    @Override
    public Record execute(Record record) {
        return record;
    }
}
