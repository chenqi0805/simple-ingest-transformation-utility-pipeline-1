package com.awslabs.aws.ti.examples.file;

import com.awslabs.aws.ti.Record;
import com.awslabs.aws.ti.buffer.TIBuffer;
import com.awslabs.aws.ti.pipeline.Pipeline;
import com.awslabs.aws.ti.processor.Processor;
import com.awslabs.aws.ti.sink.Sink;
import com.awslabs.aws.ti.source.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class FilePipeline implements Pipeline {
    private final String id;
    private final Source source;
    private final TIBuffer buffer;
    private final Processor processor;
    private final Sink sink;

    public FilePipeline(
            final String id,
            final Source source,
            final TIBuffer buffer,
            final Processor processor,
            final Sink sink) {
        this.id = id;
        this.source = source;
        this.buffer = buffer;
        this.processor = processor;
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
        return Optional.of(this.processor);
    }

    @Override
    public void execute() {
        source.start(buffer);
        final List<Record> records = new ArrayList<>();
        Record record;
        while((record = buffer.get()) != null) {
            records.add(processor.execute(record));
        }
        sink.output(records);
    }

    @Override
    public void stop() {
        source.stop();
        sink.stop();
    }
}
