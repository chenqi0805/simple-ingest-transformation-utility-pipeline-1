package com.amazon.ti.examples.console;

import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.buffer.InMemoryBuffer;
import com.amazon.ti.pipeline.Pipeline;
import com.amazon.ti.sink.Sink;
import com.amazon.ti.source.Source;

/**
 * Simple example to demonstrate the Pipeline.
 * Execute the class and enter records to process. type "exit" to stop reading the data
 */
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
