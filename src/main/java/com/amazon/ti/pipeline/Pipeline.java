package com.amazon.ti.pipeline;

import com.amazon.ti.Record;
import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.processor.Processor;
import com.amazon.ti.sink.Sink;
import com.amazon.ti.source.Source;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Pipeline is a data transformation flow which reads data from {@link Source}, optionally transforms the data
 * using {@link Processor} and outputs the transformed (or original) data to {@link Sink}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class Pipeline {
    private static List<Processor> EMPTY_PROCESSOR_LIST = new ArrayList<>(0);

    @Nonnull
    private final String name;
    @Nonnull
    private final Source source;
    @Nullable
    private final Buffer buffer;
    @Nullable
    private final List<Processor> processors;
    @Nonnull
    private final Collection<Sink> sinks;

    /**
     * Constructs a {@link Pipeline} object with provided {@link Source}, {@link #name}, {@link Collection} of
     * {@link Sink} and default {@link Buffer}, {@link Processor}.
     *
     * @param name   name of the pipeline
     * @param source source from where the pipeline reads the records
     * @param sinks  collection of sink's to which the transformed records need to be posted
     */
    public Pipeline(
            @Nonnull final String name,
            @Nonnull final Source source,
            @Nonnull final Collection<Sink> sinks) {
        this.name = name;
        this.source = source;
        this.buffer = Buffer.defaultBuffer();
        processors = EMPTY_PROCESSOR_LIST;
        this.sinks = sinks;
    }

    /**
     * Constructs a {@link Pipeline} object with provided {@link Source}, {@link #name}, {@link Collection} of
     * {@link Sink}, {@link Buffer} and list of {@link Processor}. On {@link #execute()} the engine will read
     * records {@link Record} from provided {@link Source}, buffers the records in {@link Buffer}, applies List of
     * {@link Processor} sequentially (in the given order) and outputs the processed records to collection of {@link
     * Sink}
     *
     * @param name       name of the pipeline
     * @param source     source from where the pipeline reads the records
     * @param buffer     buffer for the source to queue records
     * @param processors processor that is applied to records
     * @param sinks      sink to which the transformed records are posted
     */
    public Pipeline(
            @Nonnull final String name,
            @Nonnull final Source source,
            @Nullable final Buffer buffer,
            @Nullable final List<Processor> processors,
            @Nonnull final Collection<Sink> sinks) {
        this.name = name;
        this.source = source;
        this.buffer = buffer != null ? buffer : Buffer.defaultBuffer();
        this.processors = processors != null ? processors : EMPTY_PROCESSOR_LIST;
        this.sinks = sinks;
    }


    /**
     * @return Unique name of this pipeline.
     */
    @Nonnull
    public String getName() {
        return this.name;
    }

    /**
     * @return {@link Source} of this pipeline.
     */
    @Nonnull
    public Source getSource() {
        return this.source;
    }

    /**
     * @return {@link Buffer} of this pipeline.
     */
    @Nullable
    public Buffer getBuffer() {
        return this.buffer;
    }

    /**
     * @return {@link Sink} of this pipeline.
     */
    @Nonnull
    public Collection<Sink> getSinks() {
        return this.sinks;
    }

    /**
     * @return a list of {@link Processor} of this pipeline or an empty list .
     */
    @Nullable
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
        sinks.forEach(Sink::stop); //TODO wait for buffer to empty before stopping sink
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
        while ((records = buffer.readBatch()) != null && !records.isEmpty()) {
            for (final Processor processor : processors) {
                records = processor.execute(records);
            }
            postToSink(records); //TODO apply acknowledgement status to decide further processing or halting
        }
    }

    /**
     * TODO Add retry mechanism
     * TODO Add isolator pattern - Fail if one of the Sink fails [isolator Pattern]
     * TODO Update records such that sinks can modify independently [clone ?]
     */
    private boolean postToSink(Collection<Record> records) {
        sinks.forEach(sink -> sink.output(records));
        return true;
    }

}
