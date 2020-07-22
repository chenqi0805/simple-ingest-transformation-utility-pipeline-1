package com.awslabs.aws.ti.examples.console;

import com.awslabs.aws.ti.buffer.Buffer;
import com.awslabs.aws.ti.buffer.InMemoryBuffer;
import com.awslabs.aws.ti.pipeline.Pipeline;
import com.awslabs.aws.ti.sink.Sink;
import com.awslabs.aws.ti.source.Source;

public class SimpleExample {
    public static void main(String[] args) throws Exception {
        final Source consoleSource = new StdInSource();
        final Buffer inMemoryBuffer = new InMemoryBuffer();
        final Sink consoleSink = new StdOutSink();
        final Pipeline consolePipeline = new Pipeline("console-pipeline", consoleSource,
                inMemoryBuffer, consoleSink);
        consolePipeline.execute();
        consolePipeline.stop();
    }
}
