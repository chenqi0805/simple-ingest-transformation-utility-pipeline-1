package com.amazon.ti;

import com.amazon.ti.model.buffer.Buffer;
import com.amazon.ti.model.configuration.Configuration;
import com.amazon.ti.model.configuration.PluginSetting;
import com.amazon.ti.model.sink.Sink;
import com.amazon.ti.model.source.Source;
import com.amazon.ti.parser.PipelineParser;
import com.amazon.ti.parser.model.PipelineConfiguration;
import com.amazon.ti.pipeline.Pipeline;
import com.amazon.ti.plugins.buffer.BufferFactory;
import com.amazon.ti.plugins.processor.ProcessorFactory;
import com.amazon.ti.plugins.sink.SinkFactory;
import com.amazon.ti.plugins.source.SourceFactory;
import com.amazon.ti.model.processor.Processor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * DO NOT REVIEW THIS FILE YET
 * Utility class - This is solely being used for testing the partial changes.
 * TODO Either remove this class or Reformat it.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TransformationInstance {
    private static final String DEFAULT_CONFIG_LOCATION = "src/main/resources/transformation-instance.yml";
    private static final String PROCESSOR_THREADS_ATTRIBUTE = "threads";
    private Pipeline transformationPipeline;

    private static volatile TransformationInstance transformationInstance;

    public static TransformationInstance getInstance() {
        if (transformationInstance == null) {
            synchronized (TransformationInstance.class) {
                if (transformationInstance == null)
                    transformationInstance = new TransformationInstance();
            }
        }
        return transformationInstance;
    }

    private TransformationInstance() {
        if (transformationInstance != null) {
            throw new RuntimeException("Please use getInstance() for an instance of this TransformationInstance");
        }
    }

    /**
     * Executes Transformation Instance engine using the default configuration file/
     *
     * @return true if the execute successfully initiates the Transformation Instance
     */
    public boolean execute() {
        return execute(DEFAULT_CONFIG_LOCATION);
    }

    /**
     * Executes Transformation Instance engine using the default configuration file/
     *
     * @param configurationFileLocation the location of the configuration file
     * @return true if the execute successfully initiates the Transformation Instance
     */
    public boolean execute(final String configurationFileLocation) {
        final PipelineParser pipelineParser = new PipelineParser(configurationFileLocation);
        final PipelineConfiguration pipelineConfiguration = pipelineParser.parseConfiguration();
        execute(pipelineConfiguration);
        return true;
    }

    /**
     * Terminates the execution of Transformation Instance
     * TODO - Set a flag to notify components
     *
     * return boolean status of the stop request [TODO]
     */
    public void stop() {
        transformationPipeline.stop();
    }

    /**
     * Executes Transformation instance engine for the provided {@link PipelineConfiguration}
     *
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
     *
     * @param pipelineParser an instance of {@link PipelineParser} to retrieve {@link PipelineConfiguration}
     * @return true if the execute successfully initiates the Transformation Instance
     * {@link com.amazon.ti.pipeline.Pipeline} execute.
     */
    public boolean execute(final PipelineParser pipelineParser) {
        return execute(pipelineParser.parseConfiguration());
    }

    private Pipeline buildPipelineFromConfiguration(final PipelineConfiguration pipelineConfiguration) {
        final Source source = SourceFactory.newSource(getFirstSettingsIfExists(pipelineConfiguration.getSource()));
        final PluginSetting bufferPluginSetting = getFirstSettingsIfExists(pipelineConfiguration.getBuffer());
        final Buffer buffer = bufferPluginSetting == null ? null : BufferFactory.newBuffer(bufferPluginSetting);

        final Configuration processorConfiguration = pipelineConfiguration.getProcessor();
        final List<PluginSetting> processorPluginSettings = processorConfiguration.getPluginSettings();
        final List<Processor> processors = processorPluginSettings.stream()
                                            .map(ProcessorFactory::newProcessor)
                                            .collect(Collectors.toList());

        final List<PluginSetting> sinkPluginSettings = pipelineConfiguration.getSink().getPluginSettings();
        final Collection<Sink> sinks = sinkPluginSettings.stream().map(SinkFactory::newSink).collect(Collectors.toList());

        final int processorThreads = getConfiguredThreadsOrDefault(processorConfiguration);

        return new Pipeline(pipelineConfiguration.getName(), source, buffer, processors, sinks, processorThreads);
    }

    private PluginSetting getFirstSettingsIfExists(final Configuration configuration) {
        final List<PluginSetting> pluginSettings = configuration.getPluginSettings();
        return pluginSettings.isEmpty() ? null : pluginSettings.get(0);
    }

    private int getConfiguredThreadsOrDefault(final Configuration processorConfiguration) {
        int processorThreads = processorConfiguration.getAttributeValueAsInteger(PROCESSOR_THREADS_ATTRIBUTE);
        return processorThreads == 0 ? getDefaultProcessorThreads() : processorThreads;
    }

    /**
     * TODO Implement this to use CPU cores of the executing machine
     */
    private int getDefaultProcessorThreads() {
        return 1;
    }

}
