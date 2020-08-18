package com.amazon.ti.plugins.sink.elasticsearch;

import com.amazon.ti.Record;
import com.amazon.ti.annotations.TransformationInstancePlugin;
import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.plugins.PluginType;
import com.amazon.ti.sink.Sink;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.entity.BufferedHttpEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

import static com.amazon.ti.plugins.sink.elasticsearch.ConnectionConfiguration.*;
import static com.amazon.ti.plugins.sink.elasticsearch.IndexConfiguration.*;

@TransformationInstancePlugin(name = "elasticsearch", type = PluginType.SINK)
public class ElasticsearchSink implements Sink<Record<String>> {

  // TODO: replace with error handler?
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSink.class);

  private ElasticsearchSinkConfiguration esSinkConfig;
  private RestClient restClient;

  public ElasticsearchSink(final Configuration configuration) {
    this.esSinkConfig = readESConfig(configuration);
    try {
      start();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private ElasticsearchSinkConfiguration readESConfig(final Configuration configuration) {
    ConnectionConfiguration connectionConfiguration = readConnectionConfiguration(configuration);
    IndexConfiguration indexConfiguration = readIndexConfig(configuration);

    return new ElasticsearchSinkConfiguration.Builder()
        .withConnectionConfiguration(connectionConfiguration)
        .withIndexConfiguration(indexConfiguration)
        .build();
  }

  private ConnectionConfiguration readConnectionConfiguration(final Configuration configuration){
    ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder();
    @SuppressWarnings("unchecked")
    List<String> addresses = (List<String>)configuration.getAttributeFromMetadata(ADDRESSES);
    if (addresses != null) {
      builder = builder.withAddresses(addresses);
    }
    String username = (String)configuration.getAttributeFromMetadata(USERNAME);
    if (username != null) {
      builder = builder.withUsername(username);
    }
    String password = (String)configuration.getAttributeFromMetadata(PASSWORD);
    if (password != null) {
      builder = builder.withPassword(password);
    }
    Integer socketTimeout = (Integer)configuration.getAttributeFromMetadata(SOCKET_TIMEOUT);
    if (socketTimeout != null) {
      builder = builder.withSocketTimeout(socketTimeout);
    }
    Integer connectTimeout = (Integer)configuration.getAttributeFromMetadata(CONNECT_TIMEOUT);
    if (connectTimeout != null) {
      builder = builder.withConnectTimeout(connectTimeout);
    }

    return builder.build();
  }

  private IndexConfiguration readIndexConfig(final Configuration configuration) {
    IndexConfiguration.Builder builder = new IndexConfiguration.Builder();
    String indexType = (String)configuration.getAttributeFromMetadata(INDEX_TYPE);
    if (indexType != null) {
      builder = builder.withIndexType(indexType);
    }
    String indexAlias = (String)configuration.getAttributeFromMetadata(INDEX_ALIAS);
    if (indexAlias != null) {
      builder = builder.withIndexAlias(indexAlias);
    }
    String templateFile = (String)configuration.getAttributeFromMetadata(TEMPLATE_FILE);
    if (templateFile != null) {
      builder = builder.withTemplateFile(templateFile);
    }
    return builder.build();
  }

  public void start() throws IOException {
    restClient = esSinkConfig.getConnectionConfiguration().createClient();
    if (esSinkConfig.getIndexConfiguration().getTemplateFile() != null) {
      createIndexTemplate();
    }
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
      try {
        bulkRequest.append(
            Strings.toString(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("index")
                    .endObject()
                    .endObject()
            )
        ).append("\n").append(document).append("\n");
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
    Response response;
    HttpEntity responseEntity;
    String endPoint = esSinkConfig.getIndexConfiguration().getIndexAlias() + "/_bulk";
    Request request = new Request(HttpMethod.POST, endPoint);
    request.setJsonEntity(bulkRequest.toString());
    try {
      response = restClient.performRequest(request);
      responseEntity = new BufferedHttpEntity(response.getEntity());
      // TODO: apply retry predicate here
      responseEntity = handleRetry(HttpMethod.POST, endPoint, responseEntity);
      checkForErrors(responseEntity);

      // TODO: what if partial success?
      return true;
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public void stop() {
    if (restClient != null) {
      try {
        restClient.close();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }

  private void createIndexTemplate() throws IOException {
    // TODO: add logic here to create index template
    // QUES: how to identify index template file with index pattern accordingly?
    Response response;
    HttpEntity responseEntity;
    String indexAlias = esSinkConfig.getIndexConfiguration().getIndexAlias();
    String endPoint = String.format("_index_template/%s-index-template", indexAlias);
    ClassLoader classLoader = getClass().getClassLoader();
    String jsonFilePath = esSinkConfig.getIndexConfiguration().getTemplateFile();
    StringBuilder templateJsonBuffer = new StringBuilder();
    Files.lines(Paths.get(jsonFilePath)).forEach(s -> templateJsonBuffer.append(s).append("\n"));
    String templateJson = templateJsonBuffer.toString();
    Request request = new Request(HttpMethod.POST, endPoint);
    XContentParser parser = XContentFactory.xContent(XContentType.JSON)
        .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, templateJson);
    String jsonEntity;
    if (esSinkConfig.getIndexConfiguration().getIndexType() == IndexConstants.RAW) {
      // Add -* prefix for rollover
      jsonEntity = Strings.toString(
          XContentFactory.jsonBuilder().startObject()
              .field("index_patterns", indexAlias + "-*")
              .field("template").copyCurrentStructure(parser).endObject());
    } else {
      jsonEntity = Strings.toString(
          XContentFactory.jsonBuilder().startObject()
              .field("index_patterns", indexAlias)
              .field("template").copyCurrentStructure(parser).endObject()
      );
    }
    request.setJsonEntity(jsonEntity);
    response = restClient.performRequest(request);
    responseEntity = new BufferedHttpEntity(response.getEntity());
    // TODO: apply retry predicate here
    responseEntity = handleRetry(HttpMethod.POST, endPoint, responseEntity);
    checkForErrors(responseEntity);
  }

  private void checkAndCreateIndex() throws IOException {
    // Check alias exists
    String indexAlias = esSinkConfig.getIndexConfiguration().getIndexAlias();
    Request request = new Request(HttpMethod.HEAD, indexAlias);
    Response response = restClient.performRequest(request);
    StatusLine statusLine = response.getStatusLine();
    if (statusLine.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
      // TODO: use date as suffix?
      String initialIndexName;
      if (esSinkConfig.getIndexConfiguration().getIndexType() == IndexConstants.RAW) {
        initialIndexName = indexAlias + "-000001";
        request = new Request(HttpMethod.PUT, initialIndexName);
        String jsonContent = Strings.toString(
            XContentFactory.jsonBuilder().startObject()
                .startObject("aliases")
                .startObject(indexAlias)
                .field("is_write_index", true)
                .endObject()
                .endObject()
                .endObject()
        );
        request.setJsonEntity(jsonContent);
      } else {
        initialIndexName = indexAlias;
        request = new Request(HttpMethod.PUT, initialIndexName);
      }
      response = restClient.performRequest(request);
      HttpEntity responseEntity = new BufferedHttpEntity(response.getEntity());
      // TODO: apply retry predicate here
      responseEntity = handleRetry(HttpMethod.POST, initialIndexName, responseEntity);
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
