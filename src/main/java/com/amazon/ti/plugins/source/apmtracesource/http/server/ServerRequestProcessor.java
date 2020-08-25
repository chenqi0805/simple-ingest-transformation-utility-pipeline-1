package com.amazon.ti.plugins.source.apmtracesource.http.server;

import io.netty.handler.codec.http.HttpResponseStatus;

public interface ServerRequestProcessor<T> {

  HttpResponseStatus processMessage(T message);
}
