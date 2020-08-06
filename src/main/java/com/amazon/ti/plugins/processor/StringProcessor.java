package com.amazon.ti.plugins.processor;

import com.amazon.ti.Record;
import com.amazon.ti.annotations.TransformationInstancePlugin;
import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.plugins.PluginType;
import com.amazon.ti.processor.Processor;

import java.util.ArrayList;
import java.util.Collection;

/**
 * An simple String implementation of {@link Processor} which generates new Records with upper case content. The current
 * simpler implementation does not handle errors (if any).
 */
@TransformationInstancePlugin(name = "upper-case", type = PluginType.PROCESSOR)
public class StringProcessor implements Processor<Record<String>, Record<String>> {

    /**
     * Mandatory constructor for Transformation Instance Component - This constructor is used by Transformation instance
     * runtime engine to construct an instance of {@link StringProcessor} using an instance of {@link Configuration} which
     * has access to configuration metadata from pipeline
     * configuration file.
     *
     * @param configuration instance with metadata information from pipeline configuration file.
     */
    public StringProcessor(final Configuration configuration) {
        this();
    }

    public StringProcessor() {

    }

    @Override
    public Collection<Record<String>> execute(final Collection<Record<String>> records) {
        final Collection<Record<String>> modifiedRecords = new ArrayList<>(records.size());
        for (Record<String> record : records) {
            final String recordData = record.getData();
            modifiedRecords.add(new Record<>(recordData.toUpperCase()));
        }
        return modifiedRecords;
    }
}
