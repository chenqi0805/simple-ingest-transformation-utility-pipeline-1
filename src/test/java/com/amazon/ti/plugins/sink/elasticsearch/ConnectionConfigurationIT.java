package com.amazon.ti.plugins.sink.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ConnectionConfigurationIT extends ESRestTestCase {
  public static List<String> ADDRESSES = Arrays.stream(System.getProperty("tests.rest.cluster").split(","))
      .map(ip -> "http://" + ip).collect(Collectors.toList());

  public void testCreateClientSimple() throws IOException {
    List<HttpHost> hosts = getClusterHosts();
    ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration.Builder()
        .withAddresses(ADDRESSES)
        .withUsername("")
        .withPassword("")
        .build();
    RestClient client = connectionConfiguration.createClient();
    client.close();
  }
}
