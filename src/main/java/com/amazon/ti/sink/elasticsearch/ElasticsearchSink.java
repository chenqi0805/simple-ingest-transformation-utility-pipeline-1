package com.amazon.ti.sink.elasticsearch;

import com.amazon.ti.Record;
import com.amazon.ti.sink.Sink;
import org.apache.beam.vendor.grpc.v1p26p0.io.netty.handler.codec.http.HttpMethod;
import org.apache.http.HttpEntity;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;

public class ElasticsearchSink implements Sink<Record<String>> {
  private ElasticsearchSinkConfiguration esSinkConfig;
  private RestClient restClient;
  private ArrayList<String> batch;
  private long currentBatchSizeBytes;

  /**
   * TODO: replace {@link ElasticsearchSinkConfiguration} with configuration and then
   * parse the configuration to instantiate {@link ElasticsearchSinkConfiguration}.
   */
  ElasticsearchSink(ElasticsearchSinkConfiguration esSinkConfig) {
    this.esSinkConfig = esSinkConfig;
  }

  public void setup() throws IOException {
    restClient = esSinkConfig.getConnectionConfiguration().createClient();
  }

  public void start() throws IOException {
    batch = new ArrayList<>();
    currentBatchSizeBytes = 0;
    createIndexTemplate();
  }

  @Override
  public boolean output(Collection<Record<String>> records) {
    for (Record<String> record: records) {
      /**
       * TODO:
       * 1. How is document related to getData?
       * 2. convert document into json string
       */
      String document = record.getData();
      batch.add(String.format("{ \"index\" : { } }\n%s\n", document));
      currentBatchSizeBytes += document.getBytes(StandardCharsets.UTF_8).length; // Shall we use other standards?
      if (batch.size() >= esSinkConfig.getMaxBatchSize() || currentBatchSizeBytes >= esSinkConfig.getMaxBatchSizeBytes()) {
        try {
          flushBatch();
        } catch (IOException e) {
          // TODO: handling error
          e.printStackTrace();
        }
      }
    }
    // TODO: what if partial success?
    return true;
  }

  @Override
  public void stop() {
    try {
      flushBatch();
    } catch (IOException e) {
      // TODO: handling error
      e.printStackTrace();
    }

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
    // TODO: if we use index pattern in the connection configuration, we need to remove wildcard.
    String endPoint = String.format("_index_template/%s_template", esSinkConfig.getConnectionConfiguration().getIndex());
    // TODO: replace the hardcoded file path with the logic to select the file.
    String jsonFilePath = "src/resources/fake-index-template.json";
    String indexTemplateJson = Files.readString(Path.of(jsonFilePath));
    
    HttpEntity requestBody =
        new NStringEntity(indexTemplateJson, ContentType.APPLICATION_JSON);
    Request request = new Request(HttpMethod.POST.name(), endPoint);
    request.setEntity(requestBody);
    response = restClient.performRequest(request);
    responseEntity = new BufferedHttpEntity(response.getEntity());
    // TODO: apply retry predicate here
    responseEntity = handleRetry(HttpMethod.POST.name(), endPoint, responseEntity);
    checkForErrors(responseEntity);
  }

  private void flushBatch() throws IOException {
    if (batch.isEmpty()) {
      return;
    }
    StringBuilder bulkRequest = new StringBuilder();
    for (String json : batch) {
      bulkRequest.append(json);
    }
    batch.clear();
    currentBatchSizeBytes = 0;
    Response response;
    HttpEntity responseEntity;
    // TODO: if we use index pattern in the connection configuration, we need to replace wildcard with datetime.
    String endPoint = String.format("/%s/_bulk", esSinkConfig.getConnectionConfiguration().getIndex());
    HttpEntity requestBody =
        new NStringEntity(bulkRequest.toString(), ContentType.APPLICATION_JSON);
    Request request = new Request(HttpMethod.POST.name(), endPoint);
    request.setEntity(requestBody);
    response = restClient.performRequest(request);
    responseEntity = new BufferedHttpEntity(response.getEntity());
    // TODO: apply retry predicate here
    responseEntity = handleRetry(HttpMethod.POST.name(), endPoint, responseEntity);
    checkForErrors(responseEntity);
  }

  private HttpEntity handleRetry(String method, String endpoint, HttpEntity requestBody) {
    // TODO: add logic here
    return null;
  }

  private void checkForErrors(HttpEntity responseEntity) {
    // TODO: add logic to find errors in the response entity.
  }
}
