package com.amazon.ti.buffer;

import com.amazon.ti.Record;

import java.util.LinkedList;
import java.util.Queue;

public class UnboundedInMemoryBuffer<T extends Record<?>> implements Buffer<T> {

    private final Queue<T> queue;

    public UnboundedInMemoryBuffer() {
        this.queue = new LinkedList<>();
    }

    @Override
    public void put(final T record) {
        //throws runtime exception if buffer is full
        queue.add(record);
    }

    @Override
    public T get() {
        //returns null if the buffer is empty
        return queue.poll();
    }
}
