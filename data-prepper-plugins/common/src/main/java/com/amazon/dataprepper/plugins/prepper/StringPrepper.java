package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collection;

/**
 * An simple String implementation of {@link Prepper} which generates new Records with upper case or lowercase content. The current
 * simpler implementation does not handle errors (if any).
 */
@DataPrepperPlugin(name = "string_converter", type = PluginType.PREPPER)
public class StringPrepper implements Prepper<Record<String>, Record<String>> {

    public static final String UPPER_CASE = "upper_case";

    private final boolean upperCase;

    /**
     * Mandatory constructor for Data Prepper Component - This constructor is used by Data Prepper
     * runtime engine to construct an instance of {@link StringPrepper} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public StringPrepper(final PluginSetting pluginSetting) {
        this.upperCase = pluginSetting.getBooleanOrDefault(UPPER_CASE, true);
    }

    @Override
    public Collection<Record<String>> execute(final Collection<Record<String>> records) {
        final Collection<Record<String>> modifiedRecords = new ArrayList<>(records.size());
        for (Record<String> record : records) {
            final String recordData = record.getData();
            final String newData = upperCase? recordData.toUpperCase() : recordData.toLowerCase();
            modifiedRecords.add(new Record<>(newData));
        }
        return modifiedRecords;
    }

    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }


    @Override
    public void shutdown() {

    }
}
