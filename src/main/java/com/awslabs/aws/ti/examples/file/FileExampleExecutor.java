package com.awslabs.aws.ti.examples.file;

import com.awslabs.aws.ti.buffer.TIBuffer;
import com.awslabs.aws.ti.examples.console.InMemoryTIBuffer;
import com.awslabs.aws.ti.pipeline.Pipeline;
import com.awslabs.aws.ti.processor.Processor;
import com.awslabs.aws.ti.sink.Sink;
import com.awslabs.aws.ti.source.Source;

/**
 * A simple and raw file reading example using the Pipeline
 */
public class FileExampleExecutor {
    public static void main(String[] args) {
        final Source fileSource = new FileSource();
        final TIBuffer inMemoryQueue = new InMemoryTIBuffer();
        final Processor stringProcessor = new StringProcessor();
        final Sink fileSink = new FileSink();
        final Pipeline filePipeline = new FilePipeline("file-pipeline",
                fileSource, inMemoryQueue, stringProcessor, fileSink);
        filePipeline.execute();
    }
}
