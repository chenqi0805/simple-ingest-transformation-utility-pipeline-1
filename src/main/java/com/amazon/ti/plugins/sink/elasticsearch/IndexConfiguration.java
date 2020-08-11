package com.amazon.ti.plugins.sink.elasticsearch;

import static com.google.common.base.Preconditions.checkArgument;

public class IndexConfiguration {
  private final String indexType;

  public String getIndexType() {
    return indexType;
  }

  public static class Builder {
    private String indexType = IndexType.RAW;

    public Builder withIndexType(String indexType) {
      checkArgument(indexType != null, "indexType cannot be null.");
      checkArgument( IndexType.TYPES.contains(indexType), "Invalid indexType.");
      this.indexType = indexType;
      return this;
    }

    public IndexConfiguration build() {
      return new IndexConfiguration(this);
    }
  }

  private IndexConfiguration(Builder builder) {
    this.indexType = builder.indexType;
  }
}
