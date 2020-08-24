package com.amazon.ti.plugins.source.apmtracesource;

import com.amazon.ti.model.configuration.PluginSetting;
import com.amazon.ti.model.record.Record;
import com.amazon.ti.plugins.buffer.UnboundedInMemoryBuffer;
import com.amazon.ti.plugins.source.apmtracesource.http.server.NettyHttpConfig;
import com.amazon.ti.plugins.source.apmtracesource.http.server.NettyHttpServer;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.HashMap;

public class HttpMain {
  public static void main(String[] args) {
    ApmTraceSource apmTraceSource =  new ApmTraceSource(new PluginSetting("", new HashMap<>()));
    apmTraceSource.start(new UnboundedInMemoryBuffer<>());
    apmTraceSource.stop();

    /**final String sampleJson = "{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"analytics-service\"}},{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"java\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"opentelemetry\"}},{\"key\":\"telemetry.sdk.version\",\"value\":{\"stringValue\":\"0.8.0-SNAPSHOT\"}}]},\"instrumentationLibrarySpans\":[{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.spring-webmvc-3.1\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"CFrAgv/Pv40=\",\"parentSpanId\":\"yxwHNNFJQP0=\",\"name\":\"LoggingController.save\",\"kind\":\"INTERNAL\",\"startTimeUnixNano\":\"1597902043168792500\",\"endTimeUnixNano\":\"1597902043215953100\",\"status\":{}},{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"WsV2GFh9pxw=\",\"parentSpanId\":\"mnO/qUT5ye4=\",\"name\":\"LoggingController.save\",\"kind\":\"INTERNAL\",\"startTimeUnixNano\":\"1597902046041803300\",\"endTimeUnixNano\":\"1597902046088892200\",\"status\":{}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.apache-httpasyncclient-4.0\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"G4PRdsC1+0Y=\",\"parentSpanId\":\"CFrAgv/Pv40=\",\"name\":\"HTTP PUT\",\"kind\":\"CLIENT\",\"startTimeUnixNano\":\"1597902043175204700\",\"endTimeUnixNano\":\"1597902043205117100\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"/logs/_doc/service_1?timeout=1m\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"PUT\"}}],\"status\":{}},{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"na9fwApQsYs=\",\"parentSpanId\":\"WsV2GFh9pxw=\",\"name\":\"HTTP PUT\",\"kind\":\"CLIENT\",\"startTimeUnixNano\":\"1597902046052809200\",\"endTimeUnixNano\":\"1597902046084822500\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"/logs/_doc/service_1?timeout=1m\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"PUT\"}}],\"status\":{}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.servlet-3.0\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"yxwHNNFJQP0=\",\"name\":\"/logs\",\"kind\":\"SERVER\",\"startTimeUnixNano\":\"1597902043168010200\",\"endTimeUnixNano\":\"1597902043217170200\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"41164\"}},{\"key\":\"servlet.path\",\"value\":{\"stringValue\":\"/logs\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"7\"}},{\"key\":\"http.user_agent\",\"value\":{\"stringValue\":\"curl/7.54.0\"}},{\"key\":\"http.flavor\",\"value\":{\"stringValue\":\"HTTP/1.1\"}},{\"key\":\"servlet.context\",\"value\":{\"stringValue\":\"\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8087/logs\"}},{\"key\":\"net.peer.ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"POST\"}},{\"key\":\"http.client_ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}}],\"status\":{}},{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"mnO/qUT5ye4=\",\"name\":\"/logs\",\"kind\":\"SERVER\",\"startTimeUnixNano\":\"1597902046041011600\",\"endTimeUnixNano\":\"1597902046089556800\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"41168\"}},{\"key\":\"servlet.path\",\"value\":{\"stringValue\":\"/logs\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"7\"}},{\"key\":\"http.user_agent\",\"value\":{\"stringValue\":\"curl/7.54.0\"}},{\"key\":\"http.flavor\",\"value\":{\"stringValue\":\"HTTP/1.1\"}},{\"key\":\"servlet.context\",\"value\":{\"stringValue\":\"\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8087/logs\"}},{\"key\":\"net.peer.ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"POST\"}},{\"key\":\"http.client_ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}}],\"status\":{}}]}]}";
     try {
     ApmSpanProcessor.decodeResourceSpan(sampleJson);
     } catch (JsonProcessingException e) {
     e.printStackTrace();
     }**/

  }
}
