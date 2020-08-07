package com.amazon.ti.sink.elasticsearch;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class IndexConfiguration {
  private final String indexType;

  private final int numOfShards;

  private final int numOfReplicas;

  public String getIndexType() {
    return indexType;
  }

  public int getNumOfShards() {
    return numOfShards;
  }

  public int getNumOfReplicas() {
    return numOfReplicas;
  }

  public static class Builder {
    private String indexType = IndexType.RAW;

    private int numOfShards = 1;

    private int numOfReplicas = 1;

    public Builder withIndexType(String indexType) {
      checkArgument(indexType != null, "indexType cannot be null.");
      checkArgument( IndexType.TYPES.contains(indexType), "Invalid indexType.");
      this.indexType = indexType;
      return this;
    }

    public Builder withNumOfShards(int numOfShards) {
      checkArgument(numOfShards > 0, "numOfShards must be greater than 0.");
      checkArgument(numOfShards <= 1024, "numOfShards is limited to 1024.");
      this.numOfShards = numOfShards;
      return this;
    }

    public Builder withNumOfReplicas(int numOfReplicas) {
      checkArgument(numOfReplicas >= 0, "numOfReplicas cannot be negative.");
      this.numOfReplicas = numOfReplicas;
      return this;
    }

    public IndexConfiguration build() {
      return new IndexConfiguration(this);
    }
  }

  private IndexConfiguration(Builder builder) {
    this.indexType = builder.indexType;
    this.numOfShards = builder.numOfShards;
    this.numOfReplicas = builder.numOfReplicas;
  }
}
