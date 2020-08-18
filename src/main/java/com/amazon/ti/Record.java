package com.amazon.ti;

import java.util.Map;

/**
 * Transformation Instance record - represents the fundamental data unit of TI, the idea is to encapsulate different
 * types of data we will be supporting in TI.
 * <p>
 * TODO: The current implementation focuses on proving the bare bones for which this class only need to
 * TODO: support sample test cases.
 */
public class Record<T> {
    private final T data;
    //TODO: make a better metadata object to pull metadata with correct class and prevent null objects from being set.
    private Map<String, Object> metadata;

    //TODO: Move this to an enum or static class defining core metadata fields
    public static final String RECORD_TYPE = "record_type";

    public Record(final T data) {
        this.data = data;
    }

    public Record(final T data, final Map<String, Object> metadata) {
        this.data = data;
        //TODO: need to ensure that RECORD_TYPE is always set.
        this.metadata = metadata;
    }

    public T getData() {
        return data;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    /**
     * Provides the type of Record
     * @return the record type
     */
    public String getRecordType() {
        return this.metadata.get(RECORD_TYPE).toString();
    }
}
