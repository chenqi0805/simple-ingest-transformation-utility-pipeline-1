package com.amazon.ti.plugins.sink;

import com.amazon.ti.Record;
import com.amazon.ti.annotations.TransformationInstancePlugin;
import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.plugins.PluginType;
import com.amazon.ti.sink.Sink;

import java.util.Collection;
import java.util.Iterator;

@TransformationInstancePlugin(name = "stdout", type = PluginType.SINK)
public class StdOutSink implements Sink<Record<String>> {
    private boolean haltFlag;

    /**
     * Mandatory constructor for Transformation Instance Component - This constructor is used by Transformation instance
     * runtime engine to construct an instance of {@link StdOutSink} using an instance of {@link Configuration} which
     * has access to configuration metadata from pipeline
     * configuration file.
     *
     * @param configuration instance with metadata information from pipeline configuration file.
     */
    public StdOutSink(final Configuration configuration) {
        this();
    }

    public StdOutSink() {
        haltFlag = false;
    }

    @Override
    public boolean output(Collection<Record<String>> records) {
        final Iterator<Record<String>> iterator = records.iterator();
        while (!haltFlag && iterator.hasNext()) {
            final Record<String> record = iterator.next();
            System.out.println(record.getData());
        }
        return true;
    }

    @Override
    public void stop() {
        haltFlag = true;
    }
}
