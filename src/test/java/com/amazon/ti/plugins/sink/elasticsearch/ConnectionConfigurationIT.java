package com.amazon.ti.plugins.sink.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.List;

public class ConnectionConfigurationIT extends ESSinkRestTestCase {
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
