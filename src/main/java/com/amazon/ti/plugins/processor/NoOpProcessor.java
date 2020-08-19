package com.amazon.ti.plugins.processor;

import com.amazon.ti.model.record.Record;
import com.amazon.ti.model.annotations.TransformationInstancePlugin;
import com.amazon.ti.model.configuration.PluginSetting;
import com.amazon.ti.plugins.PluginType;
import com.amazon.ti.model.processor.Processor;

import java.util.Collection;

@TransformationInstancePlugin(name = "no-op", type = PluginType.PROCESSOR)
public class NoOpProcessor<InputT extends Record<?>> implements Processor<InputT, InputT> {

    /**
     * Mandatory constructor for Transformation Instance Component - This constructor is used by Transformation instance
     * runtime engine to construct an instance of {@link NoOpProcessor} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public NoOpProcessor(final PluginSetting pluginSetting) {
        //no op
    }

    public NoOpProcessor() {

    }

    @Override
    public Collection<InputT> execute(Collection<InputT> records) {
        return records;
    }
}
