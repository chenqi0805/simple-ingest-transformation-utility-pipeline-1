package com.amazon.ti.plugins.buffer;

import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.plugins.PluginFactory;
import com.amazon.ti.plugins.PluginRepository;

@SuppressWarnings({"rawtypes"})
public class BufferFactory extends PluginFactory {

    public static Buffer newBuffer(final Configuration configuration) {
        return (Buffer) newPlugin(configuration, PluginRepository.getBufferClass(configuration.getName()));
    }
}
