package com.amazon.ti.plugins.source.apmtracesource.http.server;

import io.netty.handler.codec.http.*;

public class ServerResponseUtil {

  public static FullHttpResponse generateResourceNotFound(final HttpVersion httpVersion) {
    final FullHttpResponse response = new DefaultFullHttpResponse(httpVersion, HttpResponseStatus.NOT_FOUND);
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
    return response;
  }

  public static FullHttpResponse generateResourceInternalError(final HttpVersion httpVersion) {
    final FullHttpResponse response = new DefaultFullHttpResponse(httpVersion, HttpResponseStatus.NOT_FOUND);
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
    return response;
  }

  public static FullHttpResponse generateResponse(final HttpResponseStatus httpResponseStatus, final HttpVersion httpVersion) {
    final FullHttpResponse response = new DefaultFullHttpResponse(httpVersion, httpResponseStatus);
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
    return response;
  }
}
