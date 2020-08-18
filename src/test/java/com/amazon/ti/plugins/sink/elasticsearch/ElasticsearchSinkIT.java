package com.amazon.ti.plugins.sink.elasticsearch;

import com.amazon.ti.configuration.Configuration;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;

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
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("addresses", HOSTS);
    metadata.put("username", "");
    metadata.put("password", "");
    Configuration configuration = new Configuration("elasticsearch", metadata);
    ElasticsearchSink sink = new ElasticsearchSink(configuration);
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
    sink = new ElasticsearchSink(configuration);
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
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("addresses", HOSTS);
    metadata.put("username", "");
    metadata.put("password", "");
    metadata.put("index_alias", testIndexAlias);
    metadata.put("template_file", testTemplateFile);
    Configuration configuration = new Configuration("elasticsearch", metadata);
    ElasticsearchSink sink = new ElasticsearchSink(configuration);
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
    sink = new ElasticsearchSink(configuration);
    // Make sure no new write index *-000001 is created under alias
    String rolloverIndexName = String.format("%s-000002", testIndexAlias);
    request = new Request(HttpMethod.GET, rolloverIndexName + "/_alias");
    response = client().performRequest(request);
    assertEquals(true, checkIsWriteIndex(EntityUtils.toString(response.getEntity()), testIndexAlias, rolloverIndexName));
    sink.stop();
  }

  public void testInstantiateSinkServiceMapDefault() throws IOException {
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("index_type", IndexConstants.SERVICE_MAP);
    metadata.put("addresses", HOSTS);
    metadata.put("username", "");
    metadata.put("password", "");
    Configuration configuration = new Configuration("elasticsearch", metadata);
    ElasticsearchSink sink = new ElasticsearchSink(configuration);
    String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.SERVICE_MAP);
    Request request = new Request(HttpMethod.HEAD, indexAlias);
    Response response = client().performRequest(request);
    assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    sink.stop();
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
