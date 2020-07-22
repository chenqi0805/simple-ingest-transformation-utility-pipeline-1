package com.amazon.ti.buffer;

import com.amazon.ti.Record;

import java.util.LinkedList;
import java.util.Queue;

public class InMemoryBuffer implements Buffer {

    private final Queue<Record> queue;

    public InMemoryBuffer() {
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
