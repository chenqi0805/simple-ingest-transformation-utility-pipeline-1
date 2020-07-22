package com.awslabs.aws.ti.examples.console;

import com.awslabs.aws.ti.Record;
import com.awslabs.aws.ti.buffer.TIBuffer;
import com.awslabs.aws.ti.pipeline.Pipeline;
import com.awslabs.aws.ti.processor.Processor;
import com.awslabs.aws.ti.sink.Sink;
import com.awslabs.aws.ti.source.Source;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Simple console pipeline with no processors.
 */
public class ConsolePipeline implements Pipeline {
    private final Source source;
    private final TIBuffer buffer;
    private final Sink sink;
    private final String id;

    //use @NonNull to prevent required components from being null
    public ConsolePipeline(final String id,
            final Source source,
            final TIBuffer buffer,
            final Sink sink) {
        this.id = id;
        this.source = source;
        this.buffer = buffer;
        this.sink = sink;
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public Source getSource() {
        return this.source;
    }

    @Override
    public Sink getSink() {
        return this.sink;
    }

    @Override
    public Optional<Processor> getProcessor() {
        return Optional.empty();
    }

    @Override
    public void execute() {
        source.start(buffer);
        //keeping it simple, no smart batching
        List<Record> records = new ArrayList<>();
        Record record = buffer.get();
        while(record != null) {
            records.add(record);
            record = buffer.get();
        }
        sink.output(records);
    }

    @Override
    public void stop() {
        source.stop();
        sink.stop();
    }
}
