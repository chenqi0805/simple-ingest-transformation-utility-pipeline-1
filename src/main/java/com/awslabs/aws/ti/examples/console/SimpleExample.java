package com.awslabs.aws.ti.examples.console;

import com.awslabs.aws.ti.buffer.TIBuffer;
import com.awslabs.aws.ti.pipeline.Pipeline;
import com.awslabs.aws.ti.sink.Sink;
import com.awslabs.aws.ti.source.Source;

public class SimpleExample {
    public static void main(String[] args) throws Exception {
        final Source consoleSource = new StdInSource();
        final TIBuffer inMemoryBuffer = new InMemoryTIBuffer();
        final Sink consoleSink = new StdOutSink();
        final Pipeline consolePipeline = new ConsolePipeline("console-pipeline", consoleSource,
                inMemoryBuffer, consoleSink);
        consolePipeline.execute();
        consolePipeline.stop();
    }
}
