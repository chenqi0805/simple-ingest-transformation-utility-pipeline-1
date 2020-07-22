package com.awslabs.aws.ti.examples.console;

import com.awslabs.aws.ti.Record;
import com.awslabs.aws.ti.buffer.TIBuffer;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class InMemoryTIBuffer implements TIBuffer {

    private final Queue<Record> queue;

    public InMemoryTIBuffer() {
        this.queue = new LinkedList<>();
    }

    @Override
    public void put(final Record record) {
        //throws runtime exception if buffer is full
        queue.add(record);
    }

    @Override
    public Record get() {
        //returns null if the buffer is empty
        return queue.poll();
    }
}
