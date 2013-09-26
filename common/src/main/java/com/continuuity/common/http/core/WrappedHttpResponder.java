package com.continuuity.common.http.core;

import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;

/**
 * Wrap HttpResponder to call post handler hook.
 */
public class WrappedHttpResponder extends HttpResponder {
  private static final Logger LOG = LoggerFactory.getLogger(WrappedHttpResponder.class);

  private final HttpResponder delegate;
  private final Iterable<? extends HandlerHook> handlerHooks;
  private final HttpRequest httpRequest;
  private final Method method;
  private HttpResponseStatus status;

  public WrappedHttpResponder(HttpResponder delegate,
                              Iterable<? extends HandlerHook> handlerHooks, HttpRequest httpRequest, Method method) {
    super(null, false);
    this.delegate = delegate;
    this.handlerHooks = handlerHooks;
    this.httpRequest = httpRequest;
    this.method = method;
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object) {
    delegate.sendJson(status, object);
    runHook(status);
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object, Type type) {
    delegate.sendJson(status, object, type);
    runHook(status);
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object, Type type, Gson gson) {
    delegate.sendJson(status, object, type, gson);
    runHook(status);
  }

  @Override
  public void sendString(HttpResponseStatus status, String data) {
    delegate.sendString(status, data);
    runHook(status);
  }

  @Override
  public void sendStatus(HttpResponseStatus status) {
    delegate.sendStatus(status);
    runHook(status);
  }

  @Override
  public void sendByteArray(HttpResponseStatus status, byte[] bytes, Multimap<String, String> headers) {
    delegate.sendByteArray(status, bytes, headers);
    runHook(status);
  }

  @Override
  public void sendBytes(HttpResponseStatus status, ByteBuffer buffer, Multimap<String, String> headers) {
    delegate.sendBytes(status, buffer, headers);
    runHook(status);
  }

  @Override
  public void sendError(HttpResponseStatus status, String errorMessage) {
    delegate.sendError(status, errorMessage);
    runHook(status);
  }

  @Override
  public void sendChunkStart(HttpResponseStatus status, Multimap<String, String> headers) {
    delegate.sendChunkStart(status, headers);
    this.status = status;
  }

  @Override
  public void sendChunk(ChannelBuffer content) {
    delegate.sendChunk(content);
  }

  @Override
  public void sendChunkEnd() {
    delegate.sendChunkEnd();
    runHook(status);
  }

  @Override
  public void sendContent(HttpResponseStatus status, ChannelBuffer content, String contentType,
                          Multimap<String, String> headers) {
    delegate.sendContent(status, content, contentType, headers);
    runHook(status);
  }

  private void runHook(HttpResponseStatus status) {
    try {
      for (HandlerHook hook : handlerHooks) {
        hook.postCall(httpRequest, status, method);
      }
    } catch (Throwable t) {
      LOG.error("Post handler hook threw exception: ", t);
    }
  }
}
