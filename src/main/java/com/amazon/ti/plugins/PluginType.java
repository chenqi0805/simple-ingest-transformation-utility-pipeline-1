package com.amazon.ti.plugins;

import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.processor.Processor;
import com.amazon.ti.sink.Sink;
import com.amazon.ti.source.Source;

public enum PluginType {
    SOURCE("source", Source.class),
    BUFFER("buffer", Buffer.class),
    PROCESSOR("processor", Processor.class),
    SINK("sink", Sink.class);

    private final String pluginName;
    private final Class<?> pluginClass;

    PluginType(final String pluginName, final Class<?> pluginClass) {
        this.pluginName = pluginName;
        this.pluginClass = pluginClass;
    }

    public String pluginName() {
        return pluginName;
    }

    public Class<?> pluginClass() {
        return pluginClass;
    }
}
