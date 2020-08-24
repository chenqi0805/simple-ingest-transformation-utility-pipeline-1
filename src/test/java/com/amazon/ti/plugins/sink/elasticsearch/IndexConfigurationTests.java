package com.amazon.ti.plugins.sink.elasticsearch;

import org.junit.Test;

import static com.amazon.ti.plugins.sink.elasticsearch.IndexConstants.CUSTOM;
import static com.amazon.ti.plugins.sink.elasticsearch.IndexConstants.RAW;
import static com.amazon.ti.plugins.sink.elasticsearch.IndexConstants.RAW_DEFAULT_TEMPLATE_FILE;
import static com.amazon.ti.plugins.sink.elasticsearch.IndexConstants.SERVICE_MAP;
import static com.amazon.ti.plugins.sink.elasticsearch.IndexConstants.SERVICE_MAP_DEFAULT_TEMPLATE_FILE;
import static com.amazon.ti.plugins.sink.elasticsearch.IndexConstants.TYPE_TO_DEFAULT_ALIAS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class IndexConfigurationTests {
  @Test
  public void testDefault() {
    IndexConfiguration indexConfiguration = new IndexConfiguration.Builder().build();
    String expTemplateFile = indexConfiguration
        .getClass().getClassLoader().getResource(RAW_DEFAULT_TEMPLATE_FILE).getFile();

    assertEquals(RAW, indexConfiguration.getIndexType());
    assertEquals(TYPE_TO_DEFAULT_ALIAS.get(RAW), indexConfiguration.getIndexAlias());
    assertEquals(expTemplateFile, indexConfiguration.getTemplateFile());
  }

  @Test
  public void testRawAPMSpan() {
    String fakeTemplateFilePath = "src/resources/dummy.json";
    IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
        .withTemplateFile(fakeTemplateFilePath)
        .build();

    assertEquals(TYPE_TO_DEFAULT_ALIAS.get(RAW), indexConfiguration.getIndexAlias());
    assertEquals(fakeTemplateFilePath, indexConfiguration.getTemplateFile());

    String testIndexAlias = "foo";
    indexConfiguration = new IndexConfiguration.Builder()
        .withIndexAlias(testIndexAlias).build();
    String expTemplateFile = indexConfiguration
        .getClass().getClassLoader().getResource(RAW_DEFAULT_TEMPLATE_FILE).getFile();

    assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
    assertEquals(expTemplateFile, indexConfiguration.getTemplateFile());
  }

  @Test
  public void testServiceMap() {
    String fakeTemplateFilePath = "src/resources/dummy.json";
    IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
        .withIndexType(SERVICE_MAP)
        .withTemplateFile(fakeTemplateFilePath)
        .build();

    assertEquals(TYPE_TO_DEFAULT_ALIAS.get(SERVICE_MAP), indexConfiguration.getIndexAlias());
    assertEquals(fakeTemplateFilePath, indexConfiguration.getTemplateFile());

    String testIndexAlias = "foo";
    indexConfiguration = new IndexConfiguration.Builder()
        .withIndexType(SERVICE_MAP)
        .withIndexAlias(testIndexAlias)
        .build();
    String expTemplateFile = indexConfiguration
        .getClass().getClassLoader().getResource(SERVICE_MAP_DEFAULT_TEMPLATE_FILE).getFile();

    assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
    assertEquals(expTemplateFile, indexConfiguration.getTemplateFile());
  }

  @Test
  public void testCustom() {
    String fakeTemplateFilePath = "src/resources/dummy.json";
    String testIndexAlias = "foo";
    IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
        .withIndexType(CUSTOM)
        .withIndexAlias(testIndexAlias)
        .withTemplateFile(fakeTemplateFilePath)
        .build();

    assertEquals(CUSTOM, indexConfiguration.getIndexType());
    assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
    assertEquals(fakeTemplateFilePath, indexConfiguration.getTemplateFile());

    IndexConfiguration.Builder invalidBuilder = new IndexConfiguration.Builder()
        .withIndexType(CUSTOM)
        .withTemplateFile(fakeTemplateFilePath);
    Exception exception = assertThrows(IllegalStateException.class, invalidBuilder::build);
    assertEquals("Missing required properties:indexAlias", exception.getMessage());
  }
}
