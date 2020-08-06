package com.amazon.ti.parser;

import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.parser.model.PipelineConfiguration;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Deserializer for pipeline configuration: Deserializes on the basis of {@link PipelineAttribute} for required check,
 * expects exactly one source, at least one sink, optional buffer and processors.
 */
public class ConfigurationDeserializer extends JsonDeserializer<PipelineConfiguration> {
    private final static ObjectMapper SIMPLE_OBJECT_MAPPER = new ObjectMapper();
    private final static TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {
    };

    @Override
    public PipelineConfiguration deserialize(JsonParser jsonParser, DeserializationContext context) {
        PipelineConfiguration pipelineConfiguration;
        try {
            final JsonNode rootNode = jsonParser.getCodec().readTree(jsonParser);
            final JsonNode pipelineNode = getAttributeNodeFromPipeline(rootNode, PipelineAttribute.PIPELINE);
            final JsonNode nameNode = getAttributeNodeFromPipeline(pipelineNode, PipelineAttribute.NAME);
            final JsonNode sourceNode = getAttributeNodeFromPipeline(pipelineNode, PipelineAttribute.SOURCE);
            final JsonNode bufferNode = getAttributeNodeFromPipeline(pipelineNode, PipelineAttribute.BUFFER);
            final JsonNode processorNode = getAttributeNodeFromPipeline(pipelineNode, PipelineAttribute.PROCESSOR);
            final JsonNode sinkNode = getAttributeNodeFromPipeline(pipelineNode, PipelineAttribute.SINK);
            pipelineConfiguration = generatePipelineConfigModel(nameNode, sourceNode, bufferNode, processorNode, sinkNode);
        } catch (IOException e) {
            throw new ParseException(e.getMessage(), e.getCause());
        }
        return pipelineConfiguration;
    }

    private PipelineConfiguration generatePipelineConfigModel(
            final JsonNode nameNode,
            final JsonNode sourceNode,
            final JsonNode bufferNode,
            final JsonNode processorNode,
            final JsonNode sinkNode) {
        final String pipelineName = nameNode == null ? null : nameNode.asText();
        final List<Configuration> sourceConfigurations = getConfigurationsOrEmpty(sourceNode);
        final List<Configuration> bufferConfigurations = getConfigurationsOrEmpty(bufferNode);
        final List<Configuration> processorConfigurations = getConfigurationsOrEmpty(processorNode);
        final List<Configuration> sinkConfigurations = getConfigurationsOrEmpty(sinkNode);
        return new PipelineConfiguration(
                pipelineName,
                getFirstConfigurationIfExists(sourceConfigurations),
                getFirstConfigurationIfExists(bufferConfigurations),
                processorConfigurations,
                sinkConfigurations);
    }

    private List<Configuration> getConfigurationsOrEmpty(final JsonNode jsonNode) {
        List<Configuration> configurations = new ArrayList<>();
        if (jsonNode != null) {
            final Iterator<String> fieldIterator = jsonNode.fieldNames();
            while (fieldIterator.hasNext()) {
                String fieldName = fieldIterator.next();
                final JsonNode fieldValueNode = jsonNode.get(fieldName);
                final Map<String, Object> metadataMap = SIMPLE_OBJECT_MAPPER.convertValue(fieldValueNode, MAP_TYPE_REFERENCE);
                configurations.add(new Configuration(fieldName, metadataMap));
            }
        }
        return configurations;
    }

    /**
     * TODO if configuration file has two sources - throw an exception
     */
    private Configuration getFirstConfigurationIfExists(final List<Configuration> configurations) {
        return configurations == null || configurations.isEmpty() ? null : configurations.get(0);
    }

    private JsonNode getAttributeNodeFromPipeline(final JsonNode pipelineNode, final PipelineAttribute pipelineAttribute) {
        return pipelineNode == null ? null : pipelineNode.get(pipelineAttribute.attributeName());
    }
}
