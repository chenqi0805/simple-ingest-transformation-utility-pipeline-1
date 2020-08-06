package com.amazon.ti.configuration;

import java.util.Map;

/**
 * Configuration settings for TI components
 *
 * TODO: Consider renaming this - its overloaded word [by @laneholloway]
 */
public class Configuration {
    private final String name;
    private final Map<String, Object> metadata;

    public Configuration(final String name, Map<String, Object> metadata) {
        this.name = name;
        this.metadata = metadata;
    }

    public String getName() {
        return name;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    /**
     * Retrieves the value of the provided attribute (if exists), null otherwise.
     * @param attribute name of the attribute
     * @return value of the attribute from the metadata
     */
    public Object getAttributeFromMetadata(final String attribute) {
        return metadata.get(attribute);
    }
}
