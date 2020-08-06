package com.amazon.ti.plugins.source;

import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.plugins.PluginFactory;
import com.amazon.ti.plugins.PluginRepository;
import com.amazon.ti.source.Source;

@SuppressWarnings({"rawtypes"})
public class SourceFactory extends PluginFactory {

    public static Source newSource(final Configuration configuration) {
        return (Source) newPlugin(configuration, PluginRepository.getSourceClass(configuration.getName()));
    }
}
