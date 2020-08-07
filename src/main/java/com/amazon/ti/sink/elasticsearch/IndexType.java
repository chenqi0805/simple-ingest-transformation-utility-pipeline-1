package com.amazon.ti.sink.elasticsearch;

import java.util.HashSet;
import java.util.Set;

public enum IndexType {
  raw("raw");

  private String value;

  IndexType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  public static Set<String> getValues() {
    Set<String> values = new HashSet<>();

    for (IndexType indexType : IndexType.values()) {
      values.add(indexType.getValue());
    }
    return values;
  }
}
