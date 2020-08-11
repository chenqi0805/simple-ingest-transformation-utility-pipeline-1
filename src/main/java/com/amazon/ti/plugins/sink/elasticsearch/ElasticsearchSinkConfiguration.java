package com.amazon.ti.plugins.sink.elasticsearch;

import static com.google.common.base.Preconditions.checkArgument;

public class ElasticsearchSinkConfiguration {
  /**
   * TODO: add retryConfiguration
   */
  private final ConnectionConfiguration connectionConfiguration;

  private final IndexConfiguration indexConfiguration;

  public ConnectionConfiguration getConnectionConfiguration() {
    return connectionConfiguration;
  }

  public IndexConfiguration getIndexConfiguration() {
    return indexConfiguration;
  }

  public static class Builder {
    private ConnectionConfiguration connectionConfiguration;

    private IndexConfiguration indexConfiguration = new IndexConfiguration.Builder().build();

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
  }
}
