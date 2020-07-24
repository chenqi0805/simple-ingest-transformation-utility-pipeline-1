package com.amazon.ti.pipeline;

import com.amazon.ti.Record;
import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.processor.NoOpProcessor;
import com.amazon.ti.processor.Processor;
import com.amazon.ti.sink.Sink;
import com.amazon.ti.source.Source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Pipeline is a data transformation flow which reads data from {@link Source}, optionally transforms the data
 * using {@link Processor} and outputs the transformed (or original) data to {@link Sink}.
 *
 * TODO: Add dependencies - for guards like @NonNull
 */
public class Pipeline<InputT extends Record<?>, OutputT extends Record<?>> {

    private final String name;
    private final Source<InputT> source;
    private final Buffer<InputT> buffer;
    private final Processor<InputT, OutputT> processor;
    private final Sink<OutputT> sink;

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
            final Source<InputT> source,
            final Buffer<InputT> buffer,
            final Sink<OutputT> sink) {
        this.name = name;
        this.source = source;
        this.buffer = buffer;
        processor = null;
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
            final Source<InputT> source,
            final Buffer<InputT> buffer,
            final Processor<InputT, OutputT> processor,
            final Sink<OutputT> sink) {
        this.name = name;
        this.source = source;
        this.buffer = buffer;
        this.processor = processor;
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
    public Source<InputT> getSource() {
        return this.source;
    }

    /**
     * @return {@link Buffer} of this pipeline.
     */
    public Buffer<InputT> getBuffer() {
        return this.buffer;
    }

    /**
     * @return {@link Sink} of this pipeline.
     */
    public Sink<OutputT> getSink() {
        return this.sink;
    }

    /**
     * @return An optional {@link Processor} of this pipeline.
     */
    Optional<Processor<InputT, OutputT>> getProcessor() {
        return Optional.ofNullable(processor);
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
        final Processor<InputT, OutputT> processor = this.getProcessor().orElse(new NoOpProcessor());
        List<OutputT> records = new ArrayList<>(); //TODO: derive size from the configuration
        int index = 0;
        InputT record;
        while ((record = buffer.get()) != null) {
            records.add(index, processor.execute(record));
            if (index == bufferSize - 1) {
                postToSink(records); //no retry
                records = new ArrayList<>();
                index = -1;
            }
            index++;
        }
        if (index > 0) {
            postToSink(records);
        }
    }

    private boolean postToSink(Collection<OutputT> records) {
        return sink.output(records);
    }

}
