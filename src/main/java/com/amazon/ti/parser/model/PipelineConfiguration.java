package com.amazon.ti.parser.model;

import com.amazon.ti.model.configuration.Configuration;
import com.amazon.ti.parser.ConfigurationDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@JsonDeserialize(using = ConfigurationDeserializer.class)
public class PipelineConfiguration {

    @NotEmpty(message = "Pipeline name cannot be null or empty")
    private String name;

    @NotNull(message = "Pipeline source cannot be null")
    private Configuration source;

    @NotNull(message = "Pipeline buffer cannot be null")
    private Configuration buffer;

    @NotNull(message = "Pipeline processor cannot be null")
    private Configuration processor;

    @NotNull(message = "Pipeline sink cannot be null or empty")
    private Configuration sink;

    public PipelineConfiguration(
            String name,
            Configuration source,
            Configuration buffer,
            Configuration processor,
            Configuration sink) {
        this.name = name;
        this.source = source;
        this.buffer = buffer;
        this.processor = processor;
        this.sink = sink;
    }

    public String getName() {
        return name;
    }

    public Configuration getSource() {
        return source;
    }

    public Configuration getBuffer() {
        return buffer;
    }

    public Configuration getProcessor() {
        return processor;
    }

    public Configuration getSink() {
        return sink;
    }

}
