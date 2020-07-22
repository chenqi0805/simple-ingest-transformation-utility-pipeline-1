package com.awslabs.aws.ti.examples.console;

import com.awslabs.aws.ti.Record;
import com.awslabs.aws.ti.sink.Sink;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;

public class StdOutSink implements Sink {
    private boolean haltFlag;

    public StdOutSink() {
        haltFlag = false;
    }

    @Override
    public boolean output(Collection<Record> records) {
        final Iterator<Record> iterator = records.iterator();
        while(!haltFlag && iterator.hasNext()) {
            final Record record = iterator.next();
            System.out.println(new String(record.getData().array(), StandardCharsets.UTF_8));
        }
        return true;
    }

    @Override
    public void stop() {
        haltFlag = true;
    }
}
