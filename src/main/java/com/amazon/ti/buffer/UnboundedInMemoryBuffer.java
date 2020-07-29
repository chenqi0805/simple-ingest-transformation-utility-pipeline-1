package com.amazon.ti.buffer;

import com.amazon.ti.Record;

import java.util.*;

/**
 * Implementation of {@link Buffer} - An unbounded in-memory FIFO buffer. The bufferSize determines the size of the
 * collection for {@link #records()}.
 * @param <T> a sub-class of {@link Record}
 */
public class UnboundedInMemoryBuffer<T extends Record<?>> implements Buffer<T> {
    private static final int DEFAULT_BUFFER_SIZE = 11;

    private final Queue<T> queue;
    private final int bufferSize;

    public UnboundedInMemoryBuffer() {
        this.queue = new LinkedList<>();
        bufferSize = DEFAULT_BUFFER_SIZE;
    }

    /**
     * Constructs an unbounded in-memory buffer with provided bufferSize. The bufferSize determines the size of the
     * collection for {@link #records()}.
     * @param bufferSize
     */
    public UnboundedInMemoryBuffer(int bufferSize){
        this.queue = new LinkedList<>();
        this.bufferSize = bufferSize;
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

    /**
     * @return Collection of records, the maximum size of the collection is determined by the bufferSize (with default
     * value as 11).
     */
    @Override
    public Collection<T> records() {
        final List<T> records = new ArrayList<>();
        int index = 0;
        T record;
        while(index < bufferSize && (record = this.get()) != null) {
            records.add(record);
            index++;
        }
        return records;
    }
}
