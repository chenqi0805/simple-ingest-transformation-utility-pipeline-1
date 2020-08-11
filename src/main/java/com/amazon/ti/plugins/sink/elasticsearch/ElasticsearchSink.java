package com.amazon.ti.plugins.sink.elasticsearch;

import com.amazon.ti.Record;
import com.amazon.ti.annotations.TransformationInstancePlugin;
import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.plugins.PluginType;
import com.amazon.ti.sink.Sink;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

@TransformationInstancePlugin(name = "elasticsearch", type = PluginType.SINK)
public class ElasticsearchSink implements Sink<Record<String>> {
  private ElasticsearchSinkConfiguration esSinkConfig;
  private RestClient restClient;

  public ElasticsearchSink(final Configuration configuration) {
    this.esSinkConfig = readESConfig(configuration);
    try {
      start();
    } catch (IOException e) {
      // TODO: better error handling
      e.printStackTrace();
    }
  }

  private ElasticsearchSinkConfiguration readESConfig(Configuration configuration) {
    // TODO: add logic to convert configuration
    return null;
  }

  public void start() throws IOException {
    restClient = esSinkConfig.getConnectionConfiguration().createClient();
    createIndexTemplate();
    checkAndCreateIndex();
  }

  @Override
  public boolean output(Collection<Record<String>> records) {
    if (records.isEmpty()) {
      return false;
    }
    StringBuilder bulkRequest = new StringBuilder();
    for (Record<String> record: records) {
      /**
       * TODO:
       * If the record includes documentID, we need to fill it into the bulk request entity
       */
      String document = record.getData();
      bulkRequest.append(String.format("{ \"index\" : { } }\n%s\n", document));
    }
    Response response;
    HttpEntity responseEntity;
    // TODO: if we use index pattern in the connection configuration, we need to replace wildcard with datetime.
    String indexAlias = IndexType.TYPE_TO_ALIAS.get(esSinkConfig.getIndexConfiguration().getIndexType());
    String endPoint = String.format("/%s/_bulk", indexAlias);
    HttpEntity requestBody =
        new NStringEntity(bulkRequest.toString(), ContentType.APPLICATION_JSON);
    Request request = new Request("POST", endPoint);
    request.setEntity(requestBody);
    try {
      response = restClient.performRequest(request);
      responseEntity = new BufferedHttpEntity(response.getEntity());
      // TODO: apply retry predicate here
      responseEntity = handleRetry("POST", endPoint, responseEntity);
      checkForErrors(responseEntity);

      // TODO: what if partial success?
      return true;
    } catch (IOException e) {
      // TODO: better error handling
      e.printStackTrace();
      return false;
    }
  }

  @Override
  public void stop() {
    if (restClient != null) {
      try {
        restClient.close();
      } catch (IOException e) {
        // TODO: proper error handling
        e.printStackTrace();
      }
    }
  }

  private void createIndexTemplate() throws IOException {
    // TODO: add logic here to create index template
    // QUES: how to identify index template file with index pattern accordingly?
    Response response;
    HttpEntity responseEntity;
    String indexAlias = IndexType.TYPE_TO_ALIAS.get(IndexType.RAW);
    String endPoint = String.format("_index_template/%s-index-template", indexAlias);
    String jsonFilePath = String.format("src/resources/%s-index-template.json", indexAlias);
    // TODO: use numOfShards, numOfReplicas to replace hardcoded values in the template json string
    String indexTemplateJson = Files.readString(Path.of(jsonFilePath));
    HttpEntity requestBody =
        new NStringEntity(indexTemplateJson, ContentType.APPLICATION_JSON);
    Request request = new Request("POST", endPoint);
    request.setEntity(requestBody);
    response = restClient.performRequest(request);
    responseEntity = new BufferedHttpEntity(response.getEntity());
    // TODO: apply retry predicate here
    responseEntity = handleRetry("POST", endPoint, responseEntity);
    checkForErrors(responseEntity);
  }

  private void checkAndCreateIndex() throws IOException {
    // Check alias exists
    String indexAlias = IndexType.TYPE_TO_ALIAS.get(esSinkConfig.getIndexConfiguration().getIndexType());
    Request request = new Request("HEAD", indexAlias);
    Response response = restClient.performRequest(request);
    StatusLine statusLine = response.getStatusLine();
    if (statusLine.getStatusCode() == 404) {
      String initialIndexName = String.format("%s-000001", indexAlias);
      request = new Request("POST", initialIndexName);
      response = restClient.performRequest(request);
      HttpEntity responseEntity = new BufferedHttpEntity(response.getEntity());
      // TODO: apply retry predicate here
      responseEntity = handleRetry("POST", initialIndexName, responseEntity);
      checkForErrors(responseEntity);
    }
  }

  private HttpEntity handleRetry(String method, String endpoint, HttpEntity requestBody) {
    // TODO: add logic here
    return null;
  }

  private void checkForErrors(HttpEntity responseEntity) {
    // TODO: add logic to find errors in the response entity.
  }
}
