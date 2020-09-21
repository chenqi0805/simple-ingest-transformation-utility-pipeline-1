package com.amazon.situp.parser;

import com.amazon.situp.parser.model.PipelineConfiguration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class PipelineParser {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
            .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
    private final String configurationFileLocation;

    public PipelineParser(final String configurationFileLocation) {
        this.configurationFileLocation = configurationFileLocation;
    }

    public PipelineConfiguration parseConfiguration() {
        try {
            final Map<String, PipelineConfiguration> pipelineConfigurationMap = OBJECT_MAPPER.readValue(
                    new File(configurationFileLocation), new TypeReference<Map<String, PipelineConfiguration>>() {
                    });
            final List<String> sortedPipelineNames = PipelineConfigurationValidator.
                    validateAndSortPipelines(pipelineConfigurationMap);
            return null; //TODO parse the multiple pipelines and build the pipeline
        } catch (IOException e) {
            throw new ParseException(format("Failed to parse the configuration file %s", configurationFileLocation), e);
        }
    }
}
