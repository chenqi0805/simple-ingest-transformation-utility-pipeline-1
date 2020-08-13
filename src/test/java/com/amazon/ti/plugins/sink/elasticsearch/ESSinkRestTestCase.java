package com.amazon.ti.plugins.sink.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ESSinkRestTestCase extends ESRestTestCase {
  public static List<String> ADDRESSES = Arrays.stream(System.getProperty("tests.rest.cluster").split(","))
      .map(ip -> "http://" + ip).collect(Collectors.toList());
}
