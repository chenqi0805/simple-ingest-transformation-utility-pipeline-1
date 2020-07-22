package com.awslabs.aws.ti.pipeline;

import com.awslabs.aws.ti.Record;
import com.awslabs.aws.ti.buffer.Buffer;
import com.awslabs.aws.ti.processor.NoOpProcessor;
import com.awslabs.aws.ti.processor.Processor;
import com.awslabs.aws.ti.sink.Sink;
import com.awslabs.aws.ti.source.Source;

import java.util.*;

/**
 * Pipeline is a data transformation flow which reads data from {@link Source}, optionally transforms the data
 * using {@link Processor} and outputs the transformed (or original) data to {@link Sink}.
 */
public class Pipeline {

    private final String name;
    private final Source source;
    private final Buffer buffer;
    private final Optional<Processor> processorOptional;
    private final Sink sink;

    /**
     * Constructs a pipeline without processor
     *
     * @param name   name of the pipeline
     * @param source source from where the pipeline reads the records
     * @param buffer buffer for the source to queue records
     * @param sink   sink to which the transformed records are posted
     */
    public Pipeline(
            final String name,
            final Source source,
            final Buffer buffer,
            final Sink sink) {
        this.name = name;
        this.source = source;
        this.buffer = buffer;
        processorOptional = Optional.empty();
        this.sink = sink;
    }

    /**
     * Constructs a pipeline with processor
     *
     * @param name      name of the pipeline
     * @param source    source from where the pipeline reads the records
     * @param buffer    buffer for the source to queue records
     * @param processor processor that is applied to records
     * @param sink      sink to which the transformed records are posted
     */
    public Pipeline(
            final String name,
            final Source source,
            final Buffer buffer,
            final Processor processor,
            final Sink sink) {
        this.name = name;
        this.source = source;
        this.buffer = buffer;
        processorOptional = Optional.of(processor);
        this.sink = sink;
    }


    /**
     * @return Unique name of this pipeline.
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return {@link Source} of this pipeline.
     */
    public Source getSource() {
        return this.source;
    }

    /**
     * @return {@link Buffer} of this pipeline.
     */
    public Buffer getBuffer() {
        return this.buffer;
    }

    /**
     * @return {@link Sink} of this pipeline.
     */
    public Sink getSink() {
        return this.sink;
    }

    /**
     * @return An optional {@link Processor} of this pipeline.
     */
    Optional<Processor> getProcessor() {
        return processorOptional;
    }

    /**
     * Executes the current pipeline i.e. reads the data from {@link Source}, executes optional {@link Processor} on the
     * read data and outputs to {@link Sink}.
     */
    public void execute() {
        executeWithStart();
    }

    /**
     * Notifies the components to stop the processing.
     */
    public void stop() {
        source.stop();
        sink.stop(); //TODO wait for buffer to empty before stopping sink
    }

    private void executeWithStart() {
        source.start(buffer);
        executeToEmptyBuffer(11); //TODO: derive from the configuration
    }

    /**
     * TODO: Derive bufferSize from configuration
     */
    private void executeToEmptyBuffer(int bufferSize) {
        final Processor processor = processorOptional.orElse(new NoOpProcessor());
        List<Record> records = new ArrayList<>(); //TODO: derive size from the configuration
        int index = 0;
        Record record;
        while ((record = buffer.get()) != null) {
            records.add(index, processor.execute(record));
            if (index == bufferSize - 1) {
                postToSink(records); //no retry
                records = new ArrayList<>();
                index = -1;
            }
            index++;
        }
        if(index > 0) {
            postToSink(records);
        }
    }

    private boolean postToSink(Collection<Record> records) {
        return sink.output(records);
    }

}
