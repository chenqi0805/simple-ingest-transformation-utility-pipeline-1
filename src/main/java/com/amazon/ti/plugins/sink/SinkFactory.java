package com.amazon.ti.plugins.sink;

import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.plugins.PluginFactory;
import com.amazon.ti.plugins.PluginRepository;
import com.amazon.ti.sink.Sink;

@SuppressWarnings({"rawtypes"})
public class SinkFactory extends PluginFactory {

    public static Sink newSink(final Configuration configuration) {
        final Class<Sink> sinkClass = PluginRepository.getSinkClass(configuration.getName());
        return (Sink) newPlugin(configuration, sinkClass);
    }
}
