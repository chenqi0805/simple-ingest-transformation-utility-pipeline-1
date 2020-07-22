package com.awslabs.aws.ti;

import java.nio.ByteBuffer;

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

    public Record() {

    }

    public Record(final ByteBuffer data) {
        this.data = data;
    }

    public ByteBuffer getData() {
        return data;
    }

    public void setData(final ByteBuffer data) {
        this.data = data;
    }
}
