package com.amazon.situp.plugins.sink;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.sink.Sink;
import com.amazon.situp.pipeline.Pipeline;

import java.util.Collection;

@SitupPlugin(name = "pipeline", type = PluginType.SINK)
public class PipelineSink<T extends Record<?>> implements Sink<T> {
    private final static String ATTRIBUTE_NAME = "name";
    private final Pipeline pipeline;

    public PipelineSink(final PluginSetting pluginSetting) {
        final String pipelineName = (String) pluginSetting.getAttributeFromSettings(ATTRIBUTE_NAME);
        if(pipelineName == null || "".equals(pipelineName.trim())) {
            throw new RuntimeException("name is required for pipeline sink");
        }
        pipeline = null;

    }

    @Override
    public boolean output(Collection<T> records) {
        pipeline.getSource().start(pipeline.getBuffer());
        return false;
    }
}
