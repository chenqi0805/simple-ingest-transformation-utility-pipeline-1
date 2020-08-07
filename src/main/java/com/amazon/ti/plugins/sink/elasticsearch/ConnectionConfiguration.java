package com.amazon.ti.plugins.sink.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.KeyStore;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class ConnectionConfiguration {
  private final List<String> addresses;

  private final String username;

  private final String password;

  private final String keystorePath;

  private final String keystorePassword;

  private final Integer socketTimeout;

  private final Integer connectTimeout;

  private final boolean trustSelfSignedCerts;

  public List<String> getAddresses() {
    return addresses;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getKeystorePath() {
    return keystorePath;
  }

  public String getKeystorePassword() {
    return keystorePassword;
  }

  public Integer getSocketTimeout() {
    return socketTimeout;
  }

  public Integer getConnectTimeout() {
    return connectTimeout;
  }

  public boolean getTrustSelfSignedCerts() {
    return trustSelfSignedCerts;
  }

  public static class Builder {
    private List<String> addresses;

    private String username;

    private String password;

    private String keystorePath;

    private String keystorePassword;

    private Integer socketTimeout;

    private Integer connectTimeout;

    private boolean trustSelfSignedCerts;

    public Builder withAddresses(List<String> addresses) {
      checkArgument(addresses != null, "addresses cannot be null");
      checkArgument(addresses.size() > 0, "addresses cannot be empty list");
      this.addresses = addresses;
      return this;
    }

    public Builder withUsername(String username) {
      checkArgument(username != null, "username cannot be null");
      this.username = username;
      return this;
    }

    public Builder withPassword(String password) {
      checkArgument(password != null, "password cannot be null");
      this.password = password;
      return this;
    }

    public Builder withKeystorePath(String keystorePath) {
      checkArgument(keystorePath != null, "keystorePath cannot be null");
      this.keystorePath = keystorePath;
      return this;
    }

    public Builder withKeystorePassword(String keystorePassword) {
      checkArgument(keystorePassword != null, "keystorePassword cannot be null");
      this.keystorePassword = keystorePassword;
      return this;
    }

    public Builder withSocketTimeout(Integer socketTimeout) {
      checkArgument(socketTimeout != null, "socketTimeout cannot be null");
      this.socketTimeout = socketTimeout;
      return this;
    }

    public Builder withConnectTimeout(Integer connectTimeout) {
      checkArgument(connectTimeout != null, "connectTimeout cannot be null");
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder withTrustSelfSignedCerts(boolean trustSelfSignedCerts) {
      this.trustSelfSignedCerts = trustSelfSignedCerts;
      return this;
    }

    public ConnectionConfiguration build() {
      String missing = "";
      if (addresses == null) {
        missing += "addresses";
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }

      return new ConnectionConfiguration(this);
    }
  }

  private ConnectionConfiguration(Builder builder) {
    this.addresses = builder.addresses;
    this.username = builder.username;
    this.password = builder.password;
    this.keystorePath = builder.keystorePath;
    this.keystorePassword = builder.keystorePassword;
    this.socketTimeout = builder.socketTimeout;
    this.connectTimeout = builder.connectTimeout;
    this.trustSelfSignedCerts = builder.trustSelfSignedCerts;
  }

  public RestClient createClient() throws IOException {
    HttpHost[] hosts = new HttpHost[addresses.size()];
    int i = 0;
    for (String address : addresses) {
      URL url = new URL(address);
      hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
      i++;
    }
    RestClientBuilder restClientBuilder = RestClient.builder(hosts);
    if (username != null) {
      final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(
          AuthScope.ANY, new UsernamePasswordCredentials(username, password));
      restClientBuilder.setHttpClientConfigCallback(
          httpAsyncClientBuilder ->
              httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
    }
    if (keystorePath != null && !keystorePath.isEmpty()) {
      try {
        KeyStore keyStore = KeyStore.getInstance("jks");
        try (InputStream is = new FileInputStream(new File(keystorePath))) {
          keyStore.load(is, (keystorePassword == null) ? null : keystorePassword.toCharArray());
        }
        final TrustStrategy trustStrategy = trustSelfSignedCerts ? new TrustSelfSignedStrategy() : null;
        final SSLContext sslContext =
            SSLContexts.custom().loadTrustMaterial(keyStore, trustStrategy).build();
        final SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslContext);
        restClientBuilder.setHttpClientConfigCallback(
            httpClientBuilder ->
                httpClientBuilder.setSSLContext(sslContext).setSSLStrategy(sessionStrategy));
      } catch (Exception e) {
        throw new IOException("Can't load the client certificate from the keystore", e);
      }
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
