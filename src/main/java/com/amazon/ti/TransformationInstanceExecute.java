package com.amazon.ti;

/**
 * Execute entry into Transformation Instance.
 */
public class TransformationInstanceExecute {
    public static void main(String[] args) {
        final TransformationInstance transformationInstance = TransformationInstance.getInstance();
        boolean executeSubmissionStatus;
        if (args.length > 0) {
            executeSubmissionStatus = transformationInstance.execute(args[0]);
        } else {
            executeSubmissionStatus = transformationInstance.execute();
        }
        if (executeSubmissionStatus) {
            System.out.println("Submitted execution request successfully");
        } else {
            System.out.println("Something went wrong - Failed to submit request");
        }
    }
}