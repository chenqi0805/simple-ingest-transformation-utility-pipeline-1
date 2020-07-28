package com.amazon.ti.processor;

import com.amazon.ti.Record;

import java.util.Collection;

public class NoOpProcessor<InputT extends Record<?>> implements Processor<InputT, InputT> {

    @Override
    public Collection<InputT> execute(Collection<InputT> records) {
        return records;
    }
}
