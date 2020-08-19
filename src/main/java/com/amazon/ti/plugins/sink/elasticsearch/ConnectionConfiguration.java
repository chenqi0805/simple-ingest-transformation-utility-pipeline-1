package com.amazon.ti.plugins.sink.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class ConnectionConfiguration {
  public static final String ADDRESSES = "addresses";

  public static final String USERNAME = "username";

  public static final String PASSWORD = "password";

  public static final String SOCKET_TIMEOUT = "socket_timeout";

  public static final String CONNECT_TIMEOUT = "connect_timeout";

  private final List<String> addresses;

  private final String username;

  private final String password;

  private final Integer socketTimeout;

  private final Integer connectTimeout;

  public List<String> getAddresses() {
    return addresses;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public Integer getSocketTimeout() {
    return socketTimeout;
  }

  public Integer getConnectTimeout() {
    return connectTimeout;
  }

  public static class Builder {
    private List<String> addresses;

    private String username;

    private String password;

    private Integer socketTimeout;

    private Integer connectTimeout;

    public Builder withAddresses(final List<String> addresses) {
      checkArgument(addresses != null, "addresses cannot be null");
      checkArgument(addresses.size() > 0, "addresses cannot be empty list");
      this.addresses = addresses;
      return this;
    }

    public Builder withUsername(final String username) {
      checkArgument(username != null, "username cannot be null");
      this.username = username;
      return this;
    }

    public Builder withPassword(final String password) {
      checkArgument(password != null, "password cannot be null");
      this.password = password;
      return this;
    }

    public Builder withSocketTimeout(final Integer socketTimeout) {
      checkArgument(socketTimeout != null, "socketTimeout cannot be null");
      this.socketTimeout = socketTimeout;
      return this;
    }

    public Builder withConnectTimeout(final Integer connectTimeout) {
      checkArgument(connectTimeout != null, "connectTimeout cannot be null");
      this.connectTimeout = connectTimeout;
      return this;
    }

    public ConnectionConfiguration build() {
      String missing = "";
      if (addresses == null) {
        missing += ADDRESSES;
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }

      return new ConnectionConfiguration(this);
    }
  }

  private ConnectionConfiguration(final Builder builder) {
    this.addresses = builder.addresses;
    this.username = builder.username;
    this.password = builder.password;
    this.socketTimeout = builder.socketTimeout;
    this.connectTimeout = builder.connectTimeout;
  }

  public RestClient createClient() throws IOException {
    final HttpHost[] hosts = new HttpHost[addresses.size()];
    int i = 0;
    for (final String address : addresses) {
      hosts[i] = HttpHost.create(address);
      i++;
    }
    final RestClientBuilder restClientBuilder = RestClient.builder(hosts);
    if (username != null) {
      final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(
          AuthScope.ANY, new UsernamePasswordCredentials(username, password));
      restClientBuilder.setHttpClientConfigCallback(
          httpAsyncClientBuilder ->
              httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
    }
    restClientBuilder.setRequestConfigCallback(
        new RestClientBuilder.RequestConfigCallback() {
          @Override
          public RequestConfig.Builder customizeRequestConfig(
              RequestConfig.Builder requestConfigBuilder) {
            if (connectTimeout != null) {
              requestConfigBuilder.setConnectTimeout(connectTimeout);
            }
            if (socketTimeout != null) {
              requestConfigBuilder.setSocketTimeout(socketTimeout);
            }
            return requestConfigBuilder;
          }
        });
    return restClientBuilder.build();
  }


}
