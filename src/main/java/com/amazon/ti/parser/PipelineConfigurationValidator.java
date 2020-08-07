package com.amazon.ti.parser;

import com.amazon.ti.parser.model.PipelineConfiguration;
import org.apache.bval.jsr.ApacheValidationProvider;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.util.Set;

public class PipelineConfigurationValidator {
        private static final ValidatorFactory VALIDATOR_FACTORY = Validation
                .byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory();
        private static final Validator JSR_VALIDATOR = VALIDATOR_FACTORY.getValidator();

    /**
     * Validates the provided Pipeline configuration and exits the execution if validation fails
     * @param pipelineConfiguration Pipeline configuration for validation
     */
    public static void validate(PipelineConfiguration pipelineConfiguration) {
            final Set<ConstraintViolation<PipelineConfiguration>> violations = JSR_VALIDATOR.validate(pipelineConfiguration);
            if(violations.size() > 0) {
                violations.forEach(violation -> System.err.println(violation.getMessage())); //TODO Add Logger and exit execution here
                System.exit(1);
            }
        }

}
