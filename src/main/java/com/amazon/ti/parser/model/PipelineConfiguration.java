package com.amazon.ti.parser.model;

import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.parser.ConfigurationDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.bval.jsr.ApacheValidationProvider;

import javax.annotation.Nullable;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;

@JsonDeserialize(using = ConfigurationDeserializer.class)
public class PipelineConfiguration {

    @NotEmpty(message = "Pipeline name cannot be null or empty")
    private String name;

    @NotNull(message = "Pipeline source cannot be null")
    private Configuration source;

    @Nullable
    private Configuration buffer;
    @Nullable
    private List<Configuration> processors;

    @NotEmpty(message = "Pipeline sink cannot be null or empty")
    private List<Configuration> sink;

    public PipelineConfiguration(
            String name,
            Configuration source,
            @Nullable Configuration buffer,
            @Nullable List<Configuration> processors,
            List<Configuration> sink) {
        this.name = name;
        this.source = source;
        this.buffer = buffer;
        this.processors = processors;
        this.sink = sink;
    }

    public String getName() {
        return name;
    }

    public Configuration getSource() {
        return source;
    }

    @Nullable
    public Configuration getBuffer() {
        return buffer;
    }

    @Nullable
    public List<Configuration> getProcessors() {
        return processors;
    }

    public List<Configuration> getSink() {
        return sink;
    }

}
