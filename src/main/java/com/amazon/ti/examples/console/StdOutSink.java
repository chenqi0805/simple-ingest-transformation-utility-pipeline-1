package com.amazon.ti.examples.console;

import com.amazon.ti.Record;
import com.amazon.ti.sink.Sink;

import java.util.Collection;
import java.util.Iterator;

public class StdOutSink implements Sink<Record<String>> {
    private boolean haltFlag;

    public StdOutSink() {
        haltFlag = false;
    }

    @Override
    public boolean output(Collection<Record<String>> records) {
        final Iterator<Record<String>> iterator = records.iterator();
        while (!haltFlag && iterator.hasNext()) {
            final Record<String> record = iterator.next();
            System.out.println(record.getData());
        }
        return true;
    }

    @Override
    public void stop() {
        haltFlag = true;
    }
}
