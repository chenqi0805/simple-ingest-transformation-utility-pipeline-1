package com.amazon.ti;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Transformation Instance record - represents the fundamental data unit of TI, the idea is to encapsulate different
 * types of data we will be supporting in TI.
 * <p>
 * TODO: The current implementation focuses on proving the bare bones for which this class only need to
 * TODO: support sample test cases.
 */
public class Record {
    private ByteBuffer data;
    //private Instant instantTime;
    private Map<String,Object> metadata;

    public Record() {

    }

    public Record (final ByteBuffer data) {
        this.data = data;
    }

    public Record(final ByteBuffer data, final Map<String, Object> metadata) {
        this.data = data;
        this.metadata = metadata;
    }

    public ByteBuffer getData() {
        return data;
    }

    public void setData(final ByteBuffer data) {
        this.data = data;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
}
