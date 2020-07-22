package com.awslabs.aws.ti.pipeline;

import com.awslabs.aws.ti.processor.Processor;
import com.awslabs.aws.ti.sink.Sink;
import com.awslabs.aws.ti.source.Source;

import java.util.Optional;

/**
 * Pipeline is a data transformation flow which reads data from {@link Source}, optionally transforms the data
 * using {@link Processor} and outputs the transformed (or original) data to {@link Sink}.
 */
public interface Pipeline {

    /**
     * @return Unique Id of this pipeline.
     */
    String getId();

    /**
     * @return {@link Source} of this pipeline.
     */
    Source getSource();

    /**
     * @return {@link Sink} of this pipeline.
     */
    Sink getSink();

    /**
     * @return An optional {@link Processor} of this pipeline.
     */
    Optional<Processor> getProcessor();

    /**
     * Executes the current pipeline i.e. reads the data from {@link Source}, executes optional {@link Processor} on the
     * read data and outputs to {@link Sink}.
     */
    void execute();

    /**
     * Notifies the components to stop the processing.
     */
    void stop();

}
