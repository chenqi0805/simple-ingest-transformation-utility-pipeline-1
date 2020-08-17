package com.amazon.ti.plugins.processor;

import com.amazon.ti.Record;
import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.plugins.PluginType;
import com.amazon.ti.annotations.TransformationInstancePlugin;
import com.amazon.ti.processor.Processor;

import java.util.Collection;

@TransformationInstancePlugin(name="no-op", type = PluginType.PROCESSOR)
public class NoOpProcessor<InputT extends Record<?>> implements Processor<InputT, InputT> {

    /**
     * Mandatory constructor for Transformation Instance Component - This constructor is used by Transformation instance
     * runtime engine to construct an instance of {@link NoOpProcessor} using an instance of {@link Configuration} which
     * has access to configuration metadata from pipeline
     * configuration file.
     * @param configuration instance with metadata information from pipeline configuration file.
     */
    public NoOpProcessor(final Configuration configuration) {
        //no op
    }

    public NoOpProcessor() {

    }

    @Override
    public Collection<InputT> execute(Collection<InputT> records) {
        return records;
    }
}
