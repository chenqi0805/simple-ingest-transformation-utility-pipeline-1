package com.amazon.ti.model.configuration;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

/**
 * Configuration settings for TI components
 * <p>
 * TODO: Consider renaming this - its overloaded word [by @laneholloway]
 */
public class Configuration {
    @NotNull
    private final Map<String, Object> attributeMap;
    @NotNull
    private final List<PluginSetting> pluginSettings;

    public Configuration(
            final Map<String, Object> attributeMap,
            final List<PluginSetting> pluginSettings) {
        this.attributeMap = attributeMap;
        this.pluginSettings = pluginSettings;
    }

    public Map<String, Object> getAttributeMap() {
        return attributeMap;
    }

    public List<PluginSetting> getPluginSettings() {
        return pluginSettings;
    }

    /**
     * Retrieves the value of the provided attribute (if exists), null otherwise.
     *
     * @param attributeName name of the attribute
     * @return value of the attribute from the metadata
     */
    public Object getAttributeValue(final String attributeName) {
        return attributeMap.get(attributeName);
    }

    /**
     * Retrieves the value of the provided attribute (if exists) as String, null otherwise.
     *
     * @param attributeName name of the attribute
     * @return value of the attribute from the metadata
     */
    public String getAttributeValueAsString(final String attributeName) {
        return (String) getAttributeValue(attributeName);
    }

    /**
     * Retrieves the value of the provided attribute (if exists) as int, null otherwise.
     *
     * @param attributeName name of the attribute
     * @return value of the attribute from the metadata
     */
    public int getAttributeValueAsInteger(final String attributeName) {
        final String attributeValue = getAttributeValueAsString(attributeName);
        return attributeValue == null ? 0 : Integer.parseInt(getAttributeValueAsString(attributeName));
    }
}
