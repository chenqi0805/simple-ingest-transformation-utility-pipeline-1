package com.amazon.ti.examples.file;

import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.buffer.InMemoryBuffer;
import com.amazon.ti.pipeline.Pipeline;
import com.amazon.ti.processor.Processor;
import com.amazon.ti.sink.Sink;
import com.amazon.ti.source.Source;

/**
 * A simple and raw file reading example using the Pipeline
 */
public class FileExampleExecutor {
    public static void main(String[] args) {
        final Source fileSource = new FileSource();
        final Buffer inMemoryQueue = new InMemoryBuffer();
        final Processor stringProcessor = new StringProcessor();
        final Sink fileSink = new FileSink();
        final Pipeline filePipeline = new Pipeline("file-pipeline",
                fileSource, inMemoryQueue, stringProcessor, fileSink);
        filePipeline.execute();
    }
}
