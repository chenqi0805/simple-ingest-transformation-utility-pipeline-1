package com.amazon.ti.processor;

import com.amazon.ti.Record;

public class NoOpProcessor implements Processor {
    @Override
    public Record execute(Record record) {
        return record;
    }
}
