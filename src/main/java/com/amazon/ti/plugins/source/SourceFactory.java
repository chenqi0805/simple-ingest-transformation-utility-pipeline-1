package com.amazon.ti.plugins.source;

import com.amazon.ti.model.configuration.PluginSetting;
import com.amazon.ti.plugins.PluginFactory;
import com.amazon.ti.plugins.PluginRepository;
import com.amazon.ti.model.source.Source;

@SuppressWarnings({"rawtypes"})
public class SourceFactory extends PluginFactory {

    public static Source newSource(final PluginSetting pluginSetting) {
        return (Source) newPlugin(pluginSetting, PluginRepository.getSourceClass(pluginSetting.getName()));
    }
}
