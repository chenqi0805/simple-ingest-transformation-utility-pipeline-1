package com.amazon.ti.processor;

import com.amazon.ti.Record;

public class NoOpProcessor<InputT extends Record<?>> implements Processor<InputT, InputT> {

    @Override
    public InputT execute(InputT record) {
        return record;
    }
}
