package com.amazon.ti.parser;

import com.amazon.ti.parser.model.PipelineConfiguration;
import org.apache.bval.jsr.ApacheValidationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.util.Set;

public class PipelineConfigurationValidator {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationValidator.class);
    private static final ValidatorFactory VALIDATOR_FACTORY = Validation
            .byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory();
    private static final Validator JSR_VALIDATOR = VALIDATOR_FACTORY.getValidator();

    /**
     * Validates the provided Pipeline configuration and exits the execution if validation fails
     *
     * @param pipelineConfiguration Pipeline configuration for validation
     */
    public static void validate(PipelineConfiguration pipelineConfiguration) {
        LOG.debug("Validating pipeline configuration");
        final Set<ConstraintViolation<PipelineConfiguration>> violations = JSR_VALIDATOR.validate(pipelineConfiguration);
        if (violations.size() > 0) {
            violations.forEach(violation -> LOG.error("Found invalid configuration: {}",violation.getMessage()));
            throw new RuntimeException("Found invalid configuration, cannot proceed");
        }
    }

}
