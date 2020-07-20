package com.awslabs.aws.ti.configuration;

/**
 * Configuration settings for TI components
 */
public interface Configuration {

    /**
     * Retrieves the configuration for the component spec.
     * @param componentSpec component for which to retrieve the setting value
     * @param <T> type of the setting value to be retrieved
     * @return value of the setting for the component spec.
     * TODO change String to a component
     */
    <T> T get(final String componentSpec);
}
