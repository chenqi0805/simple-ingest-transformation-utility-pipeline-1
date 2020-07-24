package com.amazon.ti.examples.file;

import com.amazon.ti.Record;
import com.amazon.ti.processor.Processor;

public class StringProcessor implements Processor<Record<String>, Record<String>> {

    @Override
    public Record<String> execute(final Record<String> record) {
        final String recordString = record.getData();
        return new Record<>(recordString.toUpperCase());
    }
}
