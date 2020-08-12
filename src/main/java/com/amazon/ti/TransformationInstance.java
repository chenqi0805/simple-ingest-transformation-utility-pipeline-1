package com.amazon.ti;

import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.parser.PipelineParser;
import com.amazon.ti.parser.model.PipelineConfiguration;
import com.amazon.ti.pipeline.Pipeline;
import com.amazon.ti.plugins.PluginRepository;
import com.amazon.ti.plugins.buffer.BufferFactory;
import com.amazon.ti.plugins.processor.ProcessorFactory;
import com.amazon.ti.plugins.sink.SinkFactory;
import com.amazon.ti.plugins.source.SourceFactory;
import com.amazon.ti.processor.Processor;
import com.amazon.ti.sink.Sink;
import com.amazon.ti.source.Source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * DO NOT REVIEW THIS FILE YET
 * Utility class - This is solely being used for testing the partial changes.
 * TODO Either remove this class or Reformat it.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TransformationInstance {
    private static final String DEFAULT_CONFIG_LOCATION = "src/main/resources/transformation-instance.yml";
    private Pipeline transformationPipeline;

    private static volatile TransformationInstance transformationInstance;
    public static TransformationInstance getInstance(){
        if(transformationInstance == null) {
            synchronized (TransformationInstance.class){
                if(transformationInstance == null)
                    transformationInstance = new TransformationInstance();
            }
        }
        return transformationInstance;
    }

    private TransformationInstance() {
        if(transformationInstance != null) {
            throw new RuntimeException("Please use getInstance() for an instance of this TransformationInstance");
        }
    }

    /**
     * Executes Transformation Instance engine using the default configuration file/
     * @return true if the execute successfully initiates the Transformation Instance
     */
    public boolean execute() {
        final PipelineParser pipelineParser = new PipelineParser(DEFAULT_CONFIG_LOCATION);
        final PipelineConfiguration pipelineConfiguration = pipelineParser.parseConfiguration();
        execute(pipelineConfiguration);
        return true;
    }

    /**
     * Terminates the execution of Transformation Instance
     * TODO - Set a flag to notify components
     * return boolean status of the stop request [TODO]
     */
    public void stop() {
        transformationPipeline.stop();
    }

    /**
     * Executes Transformation instance engine for the provided {@link PipelineConfiguration}
     * @param pipelineConfiguration
     * @return true if the execute successfully initiates the Transformation Instance
     * {@link com.amazon.ti.pipeline.Pipeline} execute.
     */
    private boolean execute(final PipelineConfiguration pipelineConfiguration) {
        transformationPipeline = buildPipelineFromConfiguration(pipelineConfiguration);
        transformationPipeline.execute();
        return true;
    }

    /**
     * Executes Transformation instance engine using the provided {@link PipelineParser}
     * @param pipelineParser an instance of {@link PipelineParser} to retrieve {@link PipelineConfiguration}
     * @return true if the execute successfully initiates the Transformation Instance
     * {@link com.amazon.ti.pipeline.Pipeline} execute.
     */
    public boolean execute(final PipelineParser pipelineParser) {
        return execute(pipelineParser.parseConfiguration());
    }

    private Pipeline buildPipelineFromConfiguration(final PipelineConfiguration pipelineConfiguration) {
        final Source source = SourceFactory.newSource(pipelineConfiguration.getSource());

        final Configuration bufferConfiguration = pipelineConfiguration.getBuffer();
        Buffer buffer = bufferConfiguration == null ? null : BufferFactory.newBuffer(bufferConfiguration);

        final List<Configuration> processorConfigurations = pipelineConfiguration.getProcessors();
        final List<Processor> processors = processorConfigurations == null ? null : processorConfigurations
                .stream().map(ProcessorFactory::newProcessor).collect(Collectors.toList());

        final List<Configuration> sinkConfigurations = pipelineConfiguration.getSink();
        final Collection<Sink> sinks = sinkConfigurations.stream().map(SinkFactory::newSink).collect(Collectors.toList());

        return new Pipeline(pipelineConfiguration.getName(), source, buffer, processors, sinks);
    }

}
