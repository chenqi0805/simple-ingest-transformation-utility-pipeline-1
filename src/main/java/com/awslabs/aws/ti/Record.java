package com.awslabs.aws.ti;

import java.util.Map;

/**
 * Transformation Instance record interface.
 * TODO - This is still pending
 */
public interface Record {
    Map<String, Object> getData();
    Map<String, Object> getMetadata();
}
