package com.amazon.ti.plugins.sink;

import com.amazon.ti.model.configuration.PluginSetting;
import com.amazon.ti.plugins.PluginFactory;
import com.amazon.ti.plugins.PluginRepository;
import com.amazon.ti.model.sink.Sink;

@SuppressWarnings({"rawtypes"})
public class SinkFactory extends PluginFactory {

    public static Sink newSink(final PluginSetting pluginSetting) {
        return (Sink) newPlugin(pluginSetting, PluginRepository.getSinkClass(pluginSetting.getName()));
    }
}
