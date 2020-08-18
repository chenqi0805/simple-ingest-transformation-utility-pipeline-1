package com.amazon.ti;

import java.util.HashMap;

/**
 * The <b>RecordMetadata</b> class provides a wrapper around the HashMap making metadata management easier for the user.
 */
public class RecordMetadata extends HashMap<String, Object> {
    /**
     * Below are the set of keys we always expect to be present in the metadata object.
     */
    //The type of record
    public static final String KEY_RECORD_TYPE = "record_type";

    public RecordMetadata() {
        super();
    }

    /**
     * Retrieve an attribute and cast it into the simple type.
     * @param attributeName the attribute to get
     * @return the attribute as a String.
     */
    public String getAsString(String attributeName) {
        return this.get(attributeName).toString();
    }

    public static RecordMetadata defaultMetadata() {
        RecordMetadata defaultMd = new RecordMetadata();
        defaultMd.put(KEY_RECORD_TYPE, "unknown");

        return defaultMd;
    }
}
