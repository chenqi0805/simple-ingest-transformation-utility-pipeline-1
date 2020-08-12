package com.amazon.ti.plugins.buffer;

import com.amazon.ti.Record;
import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.plugins.PluginType;
import com.amazon.ti.annotations.TransformationInstancePlugin;
import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.plugins.processor.NoOpProcessor;

import java.util.*;

/**
 * Implementation of {@link Buffer} - An unbounded in-memory FIFO buffer. The bufferSize determines the size of the
 * collection for {@link #records()}.
 * @param <T> a sub-class of {@link Record}
 */
@TransformationInstancePlugin(name="unbounded-inmemory", type = PluginType.BUFFER)
public class UnboundedInMemoryBuffer<T extends Record<?>> implements Buffer<T> {
    private static final int DEFAULT_BUFFER_SIZE = 8;

    private final Queue<T> queue;
    private final int bufferSize;

    public UnboundedInMemoryBuffer() {
        this.queue = new LinkedList<>();
        bufferSize = DEFAULT_BUFFER_SIZE;
    }

    /**
     * Constructs an unbounded in-memory buffer with provided bufferSize. The bufferSize determines the size of the
     * collection for {@link #records()}.
     * @param bufferSize buffer size
     */
    public UnboundedInMemoryBuffer(int bufferSize){
        this.queue = new LinkedList<>();
        this.bufferSize = bufferSize;
    }

    /**
     * Mandatory constructor for Transformation Instance Component - This constructor is used by Transformation instance
     * runtime engine to construct an instance of {@link UnboundedInMemoryBuffer} using an instance of
     * {@link Configuration} which has access to configuration metadata from pipeline configuration file.
     * @param configuration instance with metadata information from pipeline configuration file.
     */
    public UnboundedInMemoryBuffer(final Configuration configuration) {
        this(getAttributeOrDefault("size", configuration));
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
     * value as 10).
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

    private static Integer getAttributeOrDefault(final String attribute, final Configuration configuration) {
        final Object attributeObject = configuration.getAttributeFromMetadata(attribute);
        return configuration.getAttributeFromMetadata(attribute) == null ?
                DEFAULT_BUFFER_SIZE : (Integer) attributeObject;
    }
}
