package com.amazon.ti.examples.console;

import com.amazon.ti.Record;
import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.buffer.UnboundedInMemoryBuffer;
import com.amazon.ti.pipeline.Pipeline;
import com.amazon.ti.sink.Sink;
import com.amazon.ti.source.Source;

/**
 * Simple example to demonstrate the Pipeline.
 * Execute the class and enter records to process. type "exit" to stop reading the data
 */
public class SimpleExample {
    public static void main(String[] args) {
        final Source<Record<String>> consoleSource = new StdInSource();
        final Buffer<Record<String>> inMemoryBuffer = new UnboundedInMemoryBuffer<>();
        final Sink<Record<String>> consoleSink = new StdOutSink();
        final Pipeline<Record<String>,Record<String>> consolePipeline = new Pipeline<>("console-pipeline", consoleSource,
                inMemoryBuffer, consoleSink);
        consolePipeline.execute();
        consolePipeline.stop();
    }
}
