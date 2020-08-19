package com.amazon.ti.plugins;

import com.amazon.ti.model.record.Record;
import com.amazon.ti.model.annotations.TransformationInstancePlugin;
import com.amazon.ti.model.buffer.Buffer;
import com.amazon.ti.model.source.Source;

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
