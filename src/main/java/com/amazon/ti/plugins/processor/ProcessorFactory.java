package com.amazon.ti.plugins.processor;

import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.plugins.PluginFactory;
import com.amazon.ti.plugins.PluginRepository;
import com.amazon.ti.processor.Processor;

@SuppressWarnings({"rawtypes"})
public class ProcessorFactory extends PluginFactory {

    public static Processor newProcessor(final Configuration configuration) {
        final Class<Processor> processorClass = PluginRepository.getProcessorClass(configuration.getName());
        return (Processor) newPlugin(configuration, processorClass);
    }
}
