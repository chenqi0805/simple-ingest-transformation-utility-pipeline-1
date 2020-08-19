package com.amazon.ti.plugins.buffer;

import com.amazon.ti.model.buffer.Buffer;
import com.amazon.ti.model.configuration.PluginSetting;
import com.amazon.ti.plugins.PluginFactory;
import com.amazon.ti.plugins.PluginRepository;

@SuppressWarnings({"rawtypes"})
public class BufferFactory extends PluginFactory {

    public static Buffer newBuffer(final PluginSetting pluginSetting) {
        return (Buffer) newPlugin(pluginSetting, PluginRepository.getBufferClass(pluginSetting.getName()));
    }
}
