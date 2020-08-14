package com.amazon.ti.plugins.source;

import com.amazon.ti.Record;
import com.amazon.ti.annotations.TransformationInstancePlugin;
import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.plugins.PluginType;
import com.amazon.ti.source.Source;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@TransformationInstancePlugin(name="test-source", type = PluginType.SOURCE)
public class TestSource implements Source<Record<String>> {
    public static final List<Record<String>> TEST_DATA = Stream.of("THIS", "IS", "TEST", "DATA").map(Record::new).collect(Collectors.toList());

    public TestSource(final Configuration configuration) {
    }

    public TestSource() {

    }

    @Override
    public void start(Buffer<Record<String>> buffer) {
        TEST_DATA.forEach(buffer::write);
    }

    @Override
    public void stop() {
        //not required as its not streaming but a collection.
    }
}
