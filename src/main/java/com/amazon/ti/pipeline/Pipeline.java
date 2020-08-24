package com.amazon.ti.pipeline;

import com.amazon.ti.model.record.Record;
import com.amazon.ti.model.buffer.Buffer;
import com.amazon.ti.model.processor.Processor;
import com.amazon.ti.model.sink.Sink;
import com.amazon.ti.model.source.Source;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Pipeline is a data transformation flow which reads data from {@link Source}, optionally transforms the data
 * using {@link Processor} and outputs the transformed (or original) data to {@link Sink}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class Pipeline {
    private static final List<Processor> EMPTY_PROCESSOR_LIST = new ArrayList<>(0);
    private static final int DEFAULT_TERMINATION_IN_MILLISECONDS = 5000;
    private boolean stopRequested;

    private final String name;
    private final Source source;
    private final Buffer buffer;
    private final List<Processor> processors;
    private final Collection<Sink> sinks;
    private final int processorThreads;
    private final ExecutorService executorService;

    /**
     * Constructs a {@link Pipeline} object with provided {@link Source}, {@link #name}, {@link Collection} of
     * {@link Sink} and default {@link Buffer}, {@link Processor}.
     *
     * @param name   name of the pipeline
     * @param source source from where the pipeline reads the records
     * @param sinks  collection of sink's to which the transformed records need to be posted
     * @param processorThreads configured or default threads to parallelize processor work
     */
    public Pipeline(
            @Nonnull final String name,
            @Nonnull final Source source,
            @Nonnull final Collection<Sink> sinks,
            final int processorThreads) {
        this.name = name;
        this.source = source;
        this.buffer = Buffer.defaultBuffer();
        processors = EMPTY_PROCESSOR_LIST;
        this.sinks = sinks;
        int actualProcessorThreads = processorThreads == 0 ? 1 : processorThreads;
        this.processorThreads = actualProcessorThreads;
        this.executorService = Executors.newFixedThreadPool(actualProcessorThreads);
        stopRequested = false;
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
     * @param processorThreads configured or default threads to parallelize processor work
     */
    public Pipeline(
            @Nonnull final String name,
            @Nonnull final Source source,
            @Nullable final Buffer buffer,
            @Nullable final List<Processor> processors,
            @Nonnull final Collection<Sink> sinks,
            @Nullable final int processorThreads) {
        this.name = name;
        this.source = source;
        this.buffer = buffer != null ? buffer : Buffer.defaultBuffer();
        this.processors = processors != null ? processors : EMPTY_PROCESSOR_LIST;
        this.sinks = sinks;
        int actualProcessorThreads = processorThreads == 0 ? 1 : processorThreads;
        this.processorThreads = actualProcessorThreads;
        this.executorService = Executors.newFixedThreadPool(actualProcessorThreads);
        stopRequested = false;
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
    public Collection<Sink> getSinks() {
        return this.sinks;
    }

    public boolean isStopRequested() {
        return stopRequested;
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
        stopRequested = true;
        source.stop();
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(DEFAULT_TERMINATION_IN_MILLISECONDS, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ex) {
            executorService.shutdownNow();
        }
    }

    private void executeWithStart() {
        //source.start(buffer);
        Executors.newSingleThreadExecutor().submit( () -> { source.start(buffer); });
        try {
            for (int i = 0; i < processorThreads; i++) {
                executorService.execute(new ProcessWorker(buffer, processors, sinks, this));
            }
        } catch (Exception ex) {
            executorService.shutdown();
            throw ex;
            //throw new TransformationInstanceException("Encountered exception while executing processors", ex);
        }
    }
}
