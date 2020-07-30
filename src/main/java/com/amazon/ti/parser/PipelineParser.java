package com.amazon.ti.parser;

import com.amazon.ti.parser.model.PipelineConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;

import static java.lang.String.format;

public class PipelineParser {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
    private final String configurationFileLocation;

    public PipelineParser(final String configurationFileLocation) {
        this.configurationFileLocation = configurationFileLocation;
    }

    public PipelineConfiguration parseConfiguration() {
        try {
            final PipelineConfiguration pipelineConfiguration = OBJECT_MAPPER.readValue(new File(configurationFileLocation),
                    PipelineConfiguration.class);
            PipelineConfigurationValidator.validate(pipelineConfiguration);
            return pipelineConfiguration;
        } catch (IOException e) {
            throw new ParseException(format("Failed to parse the configuration file %s", configurationFileLocation), e);
        }
    }
}
