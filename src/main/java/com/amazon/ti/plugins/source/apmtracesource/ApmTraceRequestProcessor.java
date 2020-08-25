package com.amazon.ti.plugins.source.apmtracesource;

import com.amazon.ti.model.buffer.Buffer;
import com.amazon.ti.model.record.Record;
import com.amazon.ti.plugins.source.apmtracesource.http.server.ServerRequestProcessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class ApmTraceRequestProcessor implements ServerRequestProcessor<FullHttpRequest> {
  private static final Charset CHAR_SET = StandardCharsets.UTF_8;
  private final Buffer<Record<String>> buffer;

  public ApmTraceRequestProcessor(Buffer<Record<String>> buffer) {
    this.buffer = buffer;
  }

  @Override
  public HttpResponseStatus processMessage(FullHttpRequest request) {
    final String body = request.content().toString(CHAR_SET);
    try {
      final ArrayList<String> records = ApmSpanProcessor.decodeResourceSpan(body);
      records.stream().map(Record::new).forEach(buffer::write);
    } catch (JsonProcessingException e) {
      return HttpResponseStatus.BAD_REQUEST;
    }
    return HttpResponseStatus.OK;
  }
}
