package com.amazon.ti.plugins.sink.elasticsearch;

import static com.google.common.base.Preconditions.checkArgument;

public class ElasticsearchSinkConfiguration {
  /**
   * TODO: add retryConfiguration
   */
  private final ConnectionConfiguration connectionConfiguration;

  private final IndexConfiguration indexConfiguration;

  private final long maxBatchSize;

  private final long maxBatchSizeBytes;

  public ConnectionConfiguration getConnectionConfiguration() {
    return connectionConfiguration;
  }

  public IndexConfiguration getIndexConfiguration() {
    return indexConfiguration;
  }

  public long getMaxBatchSize() {
    return maxBatchSize;
  }

  public long getMaxBatchSizeBytes() {
    return maxBatchSizeBytes;
  }

  public static class Builder {
    private ConnectionConfiguration connectionConfiguration;

    private IndexConfiguration indexConfiguration = new IndexConfiguration.Builder().build();

    private long maxBatchSize = 1000;

    private long maxBatchSizeBytes = 5L * 1024L *1024L;

    public Builder withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "connectionConfiguration cannot be null");
      this.connectionConfiguration = connectionConfiguration;
      return this;
    }

    public Builder withIndexConfiguration(IndexConfiguration indexConfiguration) {
      checkArgument(indexConfiguration != null, "indexConfiguration cannot be null");
      this.indexConfiguration = indexConfiguration;
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
      if (indexConfiguration == null) {
        missing += "indexConfiguration";
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }

      return new ElasticsearchSinkConfiguration(this);
    }
  }

  private ElasticsearchSinkConfiguration(Builder builder) {
    this.connectionConfiguration = builder.connectionConfiguration;
    this.indexConfiguration = builder.indexConfiguration;
    this.maxBatchSize = builder.maxBatchSize;
    this.maxBatchSizeBytes = builder.maxBatchSizeBytes;
  }
}
