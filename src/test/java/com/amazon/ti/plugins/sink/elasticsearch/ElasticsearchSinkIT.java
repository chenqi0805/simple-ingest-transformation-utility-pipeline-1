package com.amazon.ti.plugins.sink.elasticsearch;

import com.amazon.ti.model.configuration.PluginSetting;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.rest.ESRestTestCase;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElasticsearchSinkIT extends ESRestTestCase {
  public static List<String> HOSTS = Arrays.stream(System.getProperty("tests.rest.cluster").split(","))
      .map(ip -> "http://" + ip).collect(Collectors.toList());

  public void testInstantiateSinkRawSpanDefault() throws IOException {
    PluginSetting pluginSetting = generatePluginSetting(IndexConstants.RAW, null, null);
    ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.RAW);
    Request request = new Request(HttpMethod.HEAD, indexAlias);
    Response response = client().performRequest(request);
    assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    sink.stop();

    // roll over initial index
    request = new Request(HttpMethod.POST, String.format("%s/_rollover", indexAlias));
    request.setJsonEntity("{ \"conditions\" : { } }\n");
    response = client().performRequest(request);
    assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());

    // Instantiate sink again
    sink = new ElasticsearchSink(pluginSetting);
    // Make sure no new write index *-000001 is created under alias
    String rolloverIndexName = String.format("%s-000002", indexAlias);
    request = new Request(HttpMethod.GET, rolloverIndexName + "/_alias");
    response = client().performRequest(request);
    assertEquals(true, checkIsWriteIndex(EntityUtils.toString(response.getEntity()), indexAlias, rolloverIndexName));
    sink.stop();
  }

  public void testInstantiateSinkRawSpanCustom() throws IOException {
    String testIndexAlias = "test-raw-span";
    String testTemplateFile = getClass().getClassLoader().getResource("test-index-template.json").getFile();
    PluginSetting pluginSetting = generatePluginSetting(IndexConstants.RAW, testIndexAlias, testTemplateFile);
    ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    Request request = new Request(HttpMethod.HEAD, testIndexAlias);
    Response response = client().performRequest(request);
    assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    sink.stop();

    // roll over initial index
    request = new Request(HttpMethod.POST, String.format("%s/_rollover", testIndexAlias));
    request.setJsonEntity("{ \"conditions\" : { } }\n");
    response = client().performRequest(request);
    assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());

    // Instantiate sink again
    sink = new ElasticsearchSink(pluginSetting);
    // Make sure no new write index *-000001 is created under alias
    String rolloverIndexName = String.format("%s-000002", testIndexAlias);
    request = new Request(HttpMethod.GET, rolloverIndexName + "/_alias");
    response = client().performRequest(request);
    assertEquals(true, checkIsWriteIndex(EntityUtils.toString(response.getEntity()), testIndexAlias, rolloverIndexName));
    sink.stop();
  }

  public void testInstantiateSinkServiceMapDefault() throws IOException {
    PluginSetting pluginSetting = generatePluginSetting(IndexConstants.SERVICE_MAP, null, null);
    ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.SERVICE_MAP);
    Request request = new Request(HttpMethod.HEAD, indexAlias);
    Response response = client().performRequest(request);
    assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    sink.stop();
  }

  public void testInstantiateSinkServiceMapCustom() throws IOException {
    String testIndexAlias = "test-service-map";
    String testTemplateFile = getClass().getClassLoader().getResource("test-index-template.json").getFile();
    PluginSetting pluginSetting = generatePluginSetting(IndexConstants.SERVICE_MAP, testIndexAlias, testTemplateFile);
    ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    Request request = new Request(HttpMethod.HEAD, testIndexAlias);
    Response response = client().performRequest(request);
    assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    sink.stop();
  }

  public void testInstantiateSinkCustomIndex() throws IOException {
    String testIndexAlias = "test-alias";
    String testTemplateFile = getClass().getClassLoader().getResource("test-index-template.json").getFile();
    PluginSetting pluginSetting = generatePluginSetting(IndexConstants.CUSTOM, testIndexAlias, testTemplateFile);
    ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    Request request = new Request(HttpMethod.HEAD, testIndexAlias);
    Response response = client().performRequest(request);
    assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    sink.stop();
  }

  private PluginSetting generatePluginSetting(String indexType, String indexAlias, String templateFilePath) {
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("index_type", indexType);
    metadata.put("hosts", HOSTS);
    metadata.put("index_alias", indexAlias);
    metadata.put("template_file", templateFilePath);

    return new PluginSetting("elasticsearch", metadata);
  }

  private Boolean checkIsWriteIndex(String responseBody, String aliasName, String indexName) throws IOException {
    @SuppressWarnings("unchecked")
    Map<String, Object> indexBlob = (Map<String, Object>)createParser(XContentType.JSON.xContent(), responseBody).map().get(indexName);
    @SuppressWarnings("unchecked")
    Map<String, Object> aliasesBlob = (Map<String, Object>)indexBlob.get("aliases");
    @SuppressWarnings("unchecked")
    Map<String, Object> aliasBlob = (Map<String, Object>)aliasesBlob.get(aliasName);
    return (Boolean) aliasBlob.get("is_write_index");
  }
}
