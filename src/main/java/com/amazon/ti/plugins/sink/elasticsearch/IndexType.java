package com.amazon.ti.plugins.sink.elasticsearch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IndexType {
  public static final String RAW = "raw";

  public static final String SERVICE_MAP = "service_map";

  public static final String CUSTOM = "custom";

  public static final Set<String> TYPES = new HashSet<>();

  public static final Map<String, String> TYPE_TO_ALIAS = new HashMap<>();

  static {
    TYPES.add(RAW);
    TYPES.add(SERVICE_MAP);
    TYPES.add(CUSTOM);
    // TODO: extract out version number into version enum
    TYPE_TO_ALIAS.put(RAW, "otel-v1-apm-span");
    TYPE_TO_ALIAS.put(SERVICE_MAP, "otel-v1-apm-service-map");
  }
}
