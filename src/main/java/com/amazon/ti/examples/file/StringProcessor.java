package com.amazon.ti.examples.file;

import com.amazon.ti.Record;
import com.amazon.ti.processor.Processor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringProcessor implements Processor {

    @Override
    public Record execute(final Record record) {
        final String recordString = new String(record.getData().array(), StandardCharsets.UTF_8);
        final byte[] modifiedStringBytes = recordString.toUpperCase().getBytes(StandardCharsets.UTF_8);
        record.setData(ByteBuffer.wrap(modifiedStringBytes));
        return record;
    }
}
