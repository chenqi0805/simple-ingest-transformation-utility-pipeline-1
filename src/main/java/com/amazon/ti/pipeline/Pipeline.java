package com.amazon.ti.pipeline;

import com.amazon.ti.Record;
import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.processor.Processor;
import com.amazon.ti.sink.Sink;
import com.amazon.ti.source.Source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Pipeline is a data transformation flow which reads data from {@link Source}, optionally transforms the data
 * using {@link Processor} and outputs the transformed (or original) data to {@link Sink}.
 * TODO: Add dependencies - for guards like @NonNull
 */
public class Pipeline {

    private final String name;
    private final Source source;
    private final Buffer buffer;
    private final List<Processor> processors;
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
        processors = new ArrayList<>(0);
        this.sink = sink;
    }

    /**
     * Constructs a pipeline with processor
     *
     * @param name       name of the pipeline
     * @param source     source from where the pipeline reads the records
     * @param buffer     buffer for the source to queue records
     * @param processors processor that is applied to records
     * @param sink       sink to which the transformed records are posted
     */
    public Pipeline(
            final String name,
            final Source source,
            final Buffer buffer,
            final List<Processor> processors,
            final Sink sink) {
        this.name = name;
        this.source = source;
        this.buffer = buffer;
        this.processors = processors;
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
     * @return a list of {@link Processor} of this pipeline or an empty list .
     */
    List<Processor> getProcessors() {
        return processors;
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
        executeToEmptyBuffer();
    }

    /**
     * TODO: Pass handler to handle errors/failures
     */
    private void executeToEmptyBuffer() {
        Collection<Record> records;
        while((records = buffer.records()) != null && !records.isEmpty()) {
            for(final Processor processor : processors) {
                records = processor.execute(records);
            }
            postToSink(records); //TODO Add retry mechanism
        }
    }

    private boolean postToSink(Collection<Record> records) {
        return sink.output(records);
    }

}
