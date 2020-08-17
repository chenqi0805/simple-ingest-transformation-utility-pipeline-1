package com.amazon.ti.plugins.sink.elasticsearch;

import com.amazon.ti.configuration.Configuration;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Before;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchSinkIT extends ESSinkRestTestCase {
  private Configuration configuration;

  @Before
  public void setup() {
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("addresses", ADDRESSES);
    metadata.put("username", "");
    metadata.put("password", "");
    metadata.put("index_type", IndexType.RAW);
    configuration = new Configuration("elasticsearch", metadata);
  }

  public void testInstantiateSink() throws IOException {
    String indexAlias = IndexType.TYPE_TO_ALIAS.get(IndexType.RAW);
    ElasticsearchSink sink = new ElasticsearchSink(configuration);
    Request request = new Request(HttpMethod.HEAD, indexAlias);
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    sink.stop();

    // roll over initial index
    request = new Request(HttpMethod.POST, String.format("%s/_rollover", indexAlias));
    request.setJsonEntity("{ \"conditions\" : { } }\n");
    response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());

    // Instantiate sink again
    sink = new ElasticsearchSink(configuration);
    // Make sure no new write index *-000001 is created under alias
    String rolloverIndexName = String.format("%s-000002", indexAlias);
    request = new Request(HttpMethod.GET, rolloverIndexName + "/_alias");
    response = client().performRequest(request);
    assertEquals(true, checkIsWriteIndex(EntityUtils.toString(response.getEntity()), indexAlias, rolloverIndexName));
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
