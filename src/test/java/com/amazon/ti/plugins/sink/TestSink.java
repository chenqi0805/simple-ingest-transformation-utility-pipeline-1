package com.amazon.ti.plugins.sink;


import com.amazon.ti.model.record.Record;
import com.amazon.ti.model.annotations.TransformationInstancePlugin;
import com.amazon.ti.model.configuration.Configuration;
import com.amazon.ti.plugins.PluginType;
import com.amazon.ti.model.sink.Sink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@TransformationInstancePlugin(name = "test-sink", type = PluginType.SINK)
public class TestSink implements Sink<Record<String>> {
    private final List<Record<String>> collectedRecords;

    public TestSink(final Configuration configuration) {
        this();
    }

    public TestSink() {
        collectedRecords = new ArrayList<>();
    }

    @Override
    public boolean output(Collection<Record<String>> records) {
        records.stream().collect(Collectors.toCollection(() -> collectedRecords));
        return true;
    }

    public List<Record<String>> getCollectedRecords() {
        return collectedRecords;
    }
}
