package com.amazon.ti.plugins;

import com.amazon.ti.Record;
import com.amazon.ti.annotations.TransformationInstancePlugin;
import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.source.Source;

@TransformationInstancePlugin(name = "junit-test", type = PluginType.SOURCE)
public class ConstructorLessComponent implements Source<Record<String>> {

    @Override
    public void start(Buffer<Record<String>> buffer) {
        buffer.write(new Record<>("Junit Testing"));
    }

    @Override
    public void stop() {
        //no op
    }
}
