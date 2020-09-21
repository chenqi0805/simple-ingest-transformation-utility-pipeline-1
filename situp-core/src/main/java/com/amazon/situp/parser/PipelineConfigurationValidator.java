package com.amazon.situp.parser;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.parser.model.PipelineConfiguration;
import org.apache.bval.jsr.ApacheValidationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

public class PipelineConfigurationValidator {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationValidator.class);
    private static final String PIPELINE_ATTRIBUTE_NAME = "name";
    private static final String PIPELINE_TYPE = "pipeline";

    /**
     * Validates the provided Pipeline configuration and exits the execution if validation fails
     *
     * @param pipelineConfiguration Pipeline configuration for validation
     */
    public static void validate(final PipelineConfiguration pipelineConfiguration) {
        LOG.debug("Validating pipeline configuration");
        final ValidatorFactory validatorFactory = Validation.byProvider(ApacheValidationProvider.class)
                .configure().buildValidatorFactory();
        final Validator jsrValidator = validatorFactory.getValidator();
        final Set<ConstraintViolation<PipelineConfiguration>> violations = jsrValidator.validate(pipelineConfiguration);
        if (violations.size() > 0) {
            violations.forEach(violation -> LOG.error("Found invalid configuration: {}", violation.getMessage()));
            validatorFactory.close();
            throw new RuntimeException("Found invalid configuration, cannot proceed");
        }
        validatorFactory.close();
    }

    /**
     * Sorts the pipelines in topological order while also validating for
     * i. cycles in pipeline configuration
     * ii. incorrect pipeline source-sink configuration
     * iii. orphan pipelines
     *
     * @param pipelineConfigurationMap String to PipelineConfiguration map
     * @return List of pipeline names in topological order
     */
    public static List<String> validateAndSortPipelines(final Map<String, PipelineConfiguration> pipelineConfigurationMap) {
        final Set<String> touchedPipelineSet = new HashSet<>();
        final Set<String> visitedAndProcessedPipelineSet = new HashSet<>();
        final List<String> sortedPipelineNames = new LinkedList<>();

        pipelineConfigurationMap.forEach((pipeline, configuration) -> {
            if (!visitedAndProcessedPipelineSet.contains(pipeline)) {
                visitAndValidate(pipeline, pipelineConfigurationMap, touchedPipelineSet, visitedAndProcessedPipelineSet,
                        sortedPipelineNames);
            }
        });
        Collections.reverse(sortedPipelineNames); // reverse to put the root at the top
        validateForOrphans(sortedPipelineNames, pipelineConfigurationMap);
        return sortedPipelineNames;
    }

    private static void visitAndValidate(
            final String pipeline,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap,
            final Set<String> touchedPipelineSet,
            final Set<String> visitedAndProcessedPipelineSet,
            final List<String> sortedPipelineNames) {

        //if it is already marked, it means it results in a cycle
        if (touchedPipelineSet.contains(pipeline)) {
            LOG.error("Configuration results in a cycle - check pipeline: {}", pipeline);
            throw new RuntimeException(format("Provided configuration results in a loop, check pipeline: %s", pipeline));
        }

        //if its not already visited, recursively check
        if (!visitedAndProcessedPipelineSet.contains(pipeline)) {
            final PipelineConfiguration pipelineConfiguration = pipelineConfigurationMap.get(pipeline);
            //validate for pipeline configuration
            validate(pipelineConfiguration);
            touchedPipelineSet.add(pipeline);
            //if validation is successful, then there is definitely sink
            final List<PluginSetting> connectedPipelinesSettings = pipelineConfiguration.getSink().getPluginSettings();
            //Recursively check connected pipelines
            for (PluginSetting pluginSetting : connectedPipelinesSettings) {
                //Further process only if the sink is of pipeline type
                if (pluginSetting.getName().equals(PIPELINE_TYPE)) {
                    final String connectedPipelineName = (String) pluginSetting.getAttributeFromSettings(PIPELINE_ATTRIBUTE_NAME);
                    if (connectedPipelineName == null || "".equals(connectedPipelineName.trim())) {
                        throw new RuntimeException("name is a required attribute for sink pipeline plugin");
                    }
                    validateSourceMapping(pipeline, connectedPipelineName, pipelineConfigurationMap);
                    visitAndValidate(connectedPipelineName, pipelineConfigurationMap, touchedPipelineSet,
                            visitedAndProcessedPipelineSet, sortedPipelineNames);
                }
            }
            visitedAndProcessedPipelineSet.add(pipeline);
            touchedPipelineSet.remove(pipeline);
            sortedPipelineNames.add(pipeline);
        }
    }

    /**
     * This method validates if pipeline's source is correctly configured to reflect the sink of its parent i.e.
     * if p2 is defined as sink for p1, source of p2 should be defined as p1.
     *
     * @param sourcePipeline           name of the expected source pipeline
     * @param currentPipeline          name of the current pipeline that is being validated
     * @param pipelineConfigurationMap pipeline name to pipeline configuration map
     */
    private static void validateSourceMapping(
            final String sourcePipeline,
            final String currentPipeline,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap) {
        if (!pipelineConfigurationMap.containsKey(currentPipeline)) {
            throw new RuntimeException(format("Invalid configuration, no pipeline is defined with name %s", currentPipeline));
        }
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationMap.get(currentPipeline);
        //Current deserialization has empty source if it is not defined - so no NPE
        final List<PluginSetting> sourcePluginSettings = pipelineConfiguration.getSource().getPluginSettings();
        if (sourcePluginSettings.isEmpty() || sourcePluginSettings.get(0) == null ||
                !PIPELINE_TYPE.equals(sourcePluginSettings.get(0).getName()) ||
                !sourcePipeline.equals(sourcePluginSettings.get(0).getAttributeFromSettings(PIPELINE_ATTRIBUTE_NAME))) {
            LOG.error("Invalid configuration, expected source {} for pipeline {} is missing",
                    sourcePipeline, currentPipeline);
            throw new RuntimeException(format("Invalid configuration, expected source %s for pipeline %s is missing",
                    sourcePipeline, currentPipeline));
        }
    }

    /**
     * Validates for orphan pipeline configurations causing ambiguous execution model.
     *
     * @param sortedPipelines          pipeline names sorted in reverse order
     * @param pipelineConfigurationMap Map of pipeline name and configuration
     */
    private static void validateForOrphans(
            final List<String> sortedPipelines,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap) {
        final Set<String> expectedPipelineSet = new HashSet<>();
        //Add root pipeline name to expected set
        expectedPipelineSet.add(sortedPipelines.get(0));
        for (String currentPipelineName : sortedPipelines) {
            if (!expectedPipelineSet.contains(currentPipelineName)) {
                throw new RuntimeException("Invalid configuration, cannot proceed with ambiguous configuration");
            }
            final PipelineConfiguration pipelineConfiguration = pipelineConfigurationMap.get(currentPipelineName);
            final List<PluginSetting> pluginSettings = pipelineConfiguration.getSink().getPluginSettings();
            for (PluginSetting pluginSetting : pluginSettings) {
                if (PIPELINE_TYPE.equals(pluginSetting.getName()) &&
                        pluginSetting.getAttributeFromSettings(PIPELINE_ATTRIBUTE_NAME) != null) {
                    //Add next set of pipeline names to expected set
                    expectedPipelineSet.add((String) pluginSetting.getAttributeFromSettings(PIPELINE_ATTRIBUTE_NAME));
                }
            }
        }
    }

}
