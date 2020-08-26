package com.amazon.ti;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execute entry into Transformation Instance.
 */
public class TransformationInstanceExecute {
    private static final Logger LOG = LoggerFactory.getLogger(TransformationInstanceExecute.class);

    public static void main(String[] args) {
        final TransformationInstance transformationInstance = TransformationInstance.getInstance();
        boolean executeSubmissionStatus;
        if (args.length > 0) {
            executeSubmissionStatus = transformationInstance.execute(args[0]);
        } else {
            executeSubmissionStatus = transformationInstance.execute();
        }
        if (executeSubmissionStatus) {
            LOG.info("Submitted execution request successfully");
        } else {
            LOG.warn("Something went wrong - Failed to submit request");
        }
    }
}