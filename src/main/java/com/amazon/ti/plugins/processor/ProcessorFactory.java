package com.amazon.ti.plugins.processor;

import com.amazon.ti.model.configuration.PluginSetting;
import com.amazon.ti.plugins.PluginFactory;
import com.amazon.ti.plugins.PluginRepository;
import com.amazon.ti.model.processor.Processor;

@SuppressWarnings({"rawtypes"})
public class ProcessorFactory extends PluginFactory {

    public static Processor newProcessor(final PluginSetting pluginSetting) {
        return (Processor) newPlugin(pluginSetting, PluginRepository.getProcessorClass(pluginSetting.getName()));
    }
}
