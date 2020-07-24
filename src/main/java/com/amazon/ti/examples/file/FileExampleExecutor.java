package com.amazon.ti.examples.file;

import com.amazon.ti.Record;
import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.buffer.UnboundedInMemoryBuffer;
import com.amazon.ti.pipeline.Pipeline;
import com.amazon.ti.processor.Processor;
import com.amazon.ti.sink.Sink;
import com.amazon.ti.source.Source;

/**
 * A simple and raw file reading example using the Pipeline
 */
public class FileExampleExecutor {
    public static void main(String[] args) {
        final Source<Record<String>> fileSource = new FileSource();
        final Buffer<Record<String>> inMemoryQueue = new UnboundedInMemoryBuffer<>();
        final Processor<Record<String>, Record<String>> stringProcessor = new StringProcessor();
        final Sink<Record<String>> fileSink = new FileSink();
        final Pipeline<Record<String>,Record<String>> filePipeline = new Pipeline<>("file-pipeline",
                fileSource, inMemoryQueue, stringProcessor, fileSink);
        filePipeline.execute();
    }
}
