package com.amazon.ti.sink.elasticsearch;

import org.elasticsearch.client.RestClient;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class ElasticsearchSinkConfiguration {
  /**
   * TODO: add retryConfiguration
   */
  private final ConnectionConfiguration connectionConfiguration;

  private final long maxBatchSize;

  private final long maxBatchSizeBytes;

  public ConnectionConfiguration getConnectionConfiguration() {
    return connectionConfiguration;
  }

  public long getMaxBatchSize() {
    return maxBatchSize;
  }

  public long getMaxBatchSizeBytes() {
    return maxBatchSizeBytes;
  }

  public static class Builder {
    private ConnectionConfiguration connectionConfiguration;

    private long maxBatchSize = 1000;

    private long maxBatchSizeBytes = 5L * 1024L *1024L;

    public Builder withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "connectionConfiguration cannot be null");
      this.connectionConfiguration = connectionConfiguration;
      return this;
    }

    public Builder withMaxBatchSize(long maxBatchSize) {
      checkArgument(maxBatchSize > 0, "maxBatchSize must be > 0, but was %s", maxBatchSize);
      this.maxBatchSize = maxBatchSize;
      return this;
    }

    public Builder withMaxBatchSizeBytes(long maxBatchSizeBytes) {
      checkArgument(maxBatchSizeBytes > 0, "maxBatchSizeBytes must be > 0, but was %s", maxBatchSizeBytes);
      this.maxBatchSizeBytes = maxBatchSizeBytes;
      return this;
    }

    public ElasticsearchSinkConfiguration build() {
      String missing = "";
      if (connectionConfiguration == null) {
        missing += "connectionConfiguration";
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }

      return new ElasticsearchSinkConfiguration(this);
    }
  }

  private ElasticsearchSinkConfiguration(Builder builder) {
    this.connectionConfiguration = builder.connectionConfiguration;
    this.maxBatchSize = builder.maxBatchSize;
    this.maxBatchSizeBytes = builder.maxBatchSizeBytes;
  }
}
