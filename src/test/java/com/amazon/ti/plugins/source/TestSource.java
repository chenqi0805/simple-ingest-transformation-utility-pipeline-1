package com.amazon.ti.plugins.source;

import com.amazon.ti.model.record.Record;
import com.amazon.ti.model.annotations.TransformationInstancePlugin;
import com.amazon.ti.model.buffer.Buffer;
import com.amazon.ti.model.configuration.Configuration;
import com.amazon.ti.plugins.PluginType;
import com.amazon.ti.model.source.Source;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@TransformationInstancePlugin(name = "test-source", type = PluginType.SOURCE)
public class TestSource implements Source<Record<String>> {
    public static final List<Record<String>> TEST_DATA = Stream.of("THIS", "IS", "TEST", "DATA").map(Record::new).collect(Collectors.toList());
    private boolean isStopRequested;

    public TestSource(final Configuration configuration) {
        this();
    }

    public TestSource() {
        isStopRequested = false;
    }

    @Override
    public void start(Buffer<Record<String>> buffer) {
        final Iterator<Record<String>> iterator = TEST_DATA.iterator();
        while (iterator.hasNext() && !isStopRequested) {
            buffer.write(iterator.next());
        }
    }

    @Override
    public void stop() {
        isStopRequested = true;
    }
}
