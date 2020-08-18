package com.amazon.ti.plugins.sink.elasticsearch;

import static com.google.common.base.Preconditions.checkArgument;

public class IndexConfiguration {
  public static final String INDEX_TYPE = "index_type";

  public static final String INDEX_ALIAS = "index_alias";

  public static final String TEMPLATE_FILE = "template_file";

  private final String indexType;

  private final String indexAlias;

  private final String templateFile;

  public String getIndexType() {
    return indexType;
  }

  public String getIndexAlias() {
    return indexAlias;
  }

  public String getTemplateFile() {
    return templateFile;
  }

  public static class Builder {
    private String indexType = IndexType.RAW;

    private String indexAlias;

    private String templateFile;

    public Builder withIndexType(final String indexType) {
      checkArgument(indexType != null, "indexType cannot be null.");
      checkArgument( IndexType.TYPES.contains(indexType), "Invalid indexType.");
      this.indexType = indexType;
      return this;
    }

    public Builder withIndexAlias(final String indexAlias) {
      checkArgument(indexAlias != null, "indexAlias cannot be null.");
      checkArgument(!indexAlias.isEmpty(), "indexAlias cannot be empty");
      this.indexAlias = indexAlias;
      return this;
    }

    public Builder withTemplateFile(final String templateFile) {
      checkArgument(templateFile != null, "templateFile cannot be null.");
      this.templateFile = templateFile;
      return this;
    }

    public IndexConfiguration build() {
      return new IndexConfiguration(this);
    }
  }

  private IndexConfiguration(final Builder builder) {
    this.indexType = builder.indexType;

    String templateFile = builder.templateFile;
    if (templateFile == null) {
      if (builder.indexType == IndexType.RAW) {
        // TODO: replace magic string
        templateFile = getClass().getClassLoader()
            .getResource("otel-v1-apm-span-index-template.json").getFile();
      } else if (builder.indexType == IndexType.SERVICE_MAP) {
        // TODO: replace magic string
        templateFile = getClass().getClassLoader()
            .getResource("otel-v1-apm-service-map-index-template.json").getFile();
      }
    }
    this.templateFile = templateFile;

    String indexAlias = builder.indexAlias;
    if (indexAlias == null) {
      if (IndexType.TYPE_TO_ALIAS.containsKey(builder.indexType)) {
        indexAlias = IndexType.TYPE_TO_ALIAS.get(builder.indexType);
      } else {
        throw new IllegalStateException("Missing required properties:indexAlias");
      }
    }
    this.indexAlias = indexAlias;
  }
}
