// Copyright (c) 2019 Cloudflare, Inc. and contributors
// Licensed under the MIT License:
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#include "http-over-capnp.h"
#include <kj/debug.h>
#include <capnp/schema.h>

namespace capnp {

using kj::uint;
using kj::byte;

struct HttpOverCapnpFactory::RequestState final: public kj::Refcounted {
  kj::Maybe<kj::PromiseFulfiller<void>&> doneFulfiller;

  struct NoObject {};
  struct Done {};
  struct Canceled {};
  kj::OneOf<kj::HttpService::Response*,      // Awaiting startResponse() or startWebSocket().
            kj::Own<kj::AsyncOutputStream>,  // Response body streaming.
            kj::Own<kj::WebSocket>,          // WebSocket running.
            NoObject,                        // Running, but there's no particular state object.
            Done,                            // Finished normally.
            Canceled> state;                 // Client dropped promise, everything canceled.

  kj::Canceler canceler;

  RequestState(kj::PromiseFulfiller<void>& doneFulfiller, kj::HttpService::Response& kjResponse)
      : doneFulfiller(doneFulfiller), state(&kjResponse) {}
  RequestState(kj::Own<kj::AsyncOutputStream> stream)
      : state(kj::mv(stream)) {}
  RequestState(kj::Own<kj::WebSocket> ws)
      : state(kj::mv(ws)) {}
  RequestState()
      : state(NoObject()) {}

  template <typename T>
  kj::Maybe<T&> assertState() {
    // Assert that state.is<T>() and return a reference to it. Throws a DISCONNECTED exception if
    // in the Canceled state, or returns nullptr for any other state.
    if (state.is<T>()) {
      return state.get<T>();
    } else {
      if (state.is<Canceled>()) {
        kj::throwRecoverableException(KJ_EXCEPTION(DISCONNECTED, "request was canceled"));
      }
      return nullptr;
    }
  }
};

class HttpOverCapnpFactory::CapnpToKjStreamAdapter final: public capnp::ByteStream::Server {
  // Implemetns Cap'n Proto ByteStream as a wrapper around a KJ stream.
  //
  // TODO(perf): Detect path-shortening opportunities by using tryPumpFrom() and capturing
  //   callbacks to pumpTo() with a shorter path. If they eventually point to a
  //   KjToCapnpStreamAdapter, and if we can somehow make the capnp implementation treat this
  //   stream as a promise, then maybe we resolve ourselves to the new stream? That would allow
  //   path shortening all the way out of the process.

public:
  CapnpToKjStreamAdapter(kj::Own<RequestState> requestState)
      : requestState(kj::mv(requestState)) {}

  ~CapnpToKjStreamAdapter() noexcept(false) {
    KJ_IF_MAYBE(f, requestState->doneFulfiller) {
      if (requestState->state.is<kj::Own<kj::AsyncOutputStream>>()) {
        f->reject(KJ_EXCEPTION(DISCONNECTED, "HTTP-over-capnp server never sent response"));
      }
    }
  }

  kj::Promise<void> write(WriteContext context) override {
    auto& stream = *KJ_REQUIRE_NONNULL(
        requestState->assertState<kj::Own<kj::AsyncOutputStream>>(),
        "already called end()");

    auto data = context.getParams().getBytes();

    // TODO(now): Deal with concurrent writes, or add streaming to Cap'n Proto.
    return requestState->canceler.wrap(stream.write(data.begin(), data.size()));
  }

  kj::Promise<void> end(EndContext context) override {
    KJ_REQUIRE_NONNULL(
        requestState->assertState<kj::Own<kj::AsyncOutputStream>>(),
        "already called end()");
    requestState->state = RequestState::Done();
    KJ_IF_MAYBE(f, requestState->doneFulfiller) {
      f->fulfill();
    }
    return kj::READY_NOW;
  }

  void directEnd() {
    // Called by KjToCapnpStreamAdapter when path is shortened.

    if (requestState->state.is<kj::Own<kj::AsyncOutputStream>>()) {
      requestState->state = RequestState::Done();
      KJ_IF_MAYBE(f, requestState->doneFulfiller) {
        f->fulfill();
      }
    }
  }

  kj::Promise<void> write(const void* buffer, size_t size) {
    // Called by KjToCapnpStreamAdapter when path is shortened.

    auto& stream = *KJ_REQUIRE_NONNULL(
        requestState->assertState<kj::Own<kj::AsyncOutputStream>>(),
        "already called end()");

    return requestState->canceler.wrap(stream.write(buffer, size));
  }

  kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const byte>> pieces) {
    // Called by KjToCapnpStreamAdapter when path is shortened.

    auto& stream = *KJ_REQUIRE_NONNULL(
        requestState->assertState<kj::Own<kj::AsyncOutputStream>>(),
        "already called end()");

    return requestState->canceler.wrap(stream.write(pieces));
  }

  kj::Promise<uint64_t> pumpFrom(kj::AsyncInputStream& input, uint64_t amount) {
    // Called by KjToCapnpStreamAdapter when path is shortened.

    auto& stream = *KJ_REQUIRE_NONNULL(
        requestState->assertState<kj::Own<kj::AsyncOutputStream>>(),
        "already called end()");

    return requestState->canceler.wrap(input.pumpTo(stream, amount));
  }

private:
  kj::Own<RequestState> requestState;
};

class HttpOverCapnpFactory::KjToCapnpStreamAdapter final: public kj::AsyncOutputStream {
public:
  KjToCapnpStreamAdapter(HttpOverCapnpFactory& factory, capnp::ByteStream::Client innerParam)
      : inner(kj::mv(innerParam)),
        // If the capnp stream turns out to resolve back to this process, shorten the path.
        resolveTask(factory.streamSet.getLocalServer(inner)
            .then([this](kj::Maybe<capnp::ByteStream::Server&> server) {
          KJ_IF_MAYBE(s, server) {
            optimized = kj::downcast<CapnpToKjStreamAdapter>(*s);
          }
        }).eagerlyEvaluate(nullptr)) {}

  ~KjToCapnpStreamAdapter() noexcept(false) {
    // HACK: KJ streams are implicitly ended on destruction, but the RPC stream needs a call. We
    //   use a detached promise for now, which is probably OK since capabilities are refcounted and
    //   asynchronously destroyed anyway.
    // TODO(cleanup): Fix this when KJ streads add an explicit end() method.
    KJ_IF_MAYBE(o, optimized) {
      o->directEnd();
    } else {
      inner.endRequest().send().detach([](kj::Exception&&){});
    }
  }

  kj::Promise<void> write(const void* buffer, size_t size) override {
    KJ_IF_MAYBE(o, optimized) {
      return o->write(buffer, size);
    }

    auto req = inner.writeRequest(MessageSize { 8 + size / sizeof(word), 0 });
    req.setBytes(kj::arrayPtr(reinterpret_cast<const byte*>(buffer), size));
    return req.send().ignoreResult();
  }

  kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const byte>> pieces) override {
    KJ_IF_MAYBE(o, optimized) {
      return o->write(pieces);
    }

    size_t size = 0;
    for (auto& piece: pieces) size += piece.size();
    auto req = inner.writeRequest(MessageSize { 8 + size / sizeof(word), 0 });

    auto out = req.initBytes(size);
    byte* ptr = out.begin();
    for (auto& piece: pieces) {
      memcpy(ptr, piece.begin(), piece.size());
      ptr += piece.size();
    }
    KJ_ASSERT(ptr == out.end());

    return req.send().ignoreResult();
  }

  kj::Maybe<kj::Promise<uint64_t>> tryPumpFrom(
      kj::AsyncInputStream& input, uint64_t amount = kj::maxValue) override {
    return pumpLoop(input, 0, amount);
  }

  kj::Promise<void> whenWriteDisconnected() override {
    // TODO(soon): If resolveTask produces an exception, we could assume write is disconnecetd.
    //   To really work, CapnpToKjStreamAdapter also needs to always be treated as a promised
    //   capability that rejects when its inner stream reports disconnect.
    return kj::NEVER_DONE;
  }

private:
  capnp::ByteStream::Client inner;
  kj::Maybe<CapnpToKjStreamAdapter&> optimized;
  kj::Promise<void> resolveTask = nullptr;

  struct WriteRequestAndBuffer {
    // The order of construction/destruction of lambda captures is unspecified, but we care about
    // ordering between these two things that we want to capture, so... we need a struct.

    Request<capnp::ByteStream::WriteParams, capnp::ByteStream::WriteResults> request;
    Orphan<Data> buffer;  // points into `request`...
  };

  kj::Promise<uint64_t> pumpLoop(kj::AsyncInputStream& input,
                                 uint64_t completed, uint64_t remaining) {
    if (remaining == 0) return completed;

    KJ_IF_MAYBE(o, optimized) {
      // Oh hell yes, this capability actually points back to a stream in our own thread. We can
      // stop sending RPCs and just pump directly.
      auto promise = o->pumpFrom(input, remaining);
      if (completed > 0) {
        promise = promise.then([completed](uint64_t amount) { return amount + completed; });
      }
      return promise;
    }

    size_t size = kj::min(remaining, 8192);
    auto req = inner.writeRequest(MessageSize { 8 + size / sizeof(word) });

    auto orphanage = Orphanage::getForMessageContaining(
        capnp::ByteStream::WriteParams::Builder(req));

    auto buffer = orphanage.newOrphan<Data>(size);

    WriteRequestAndBuffer wrab = { kj::mv(req), kj::mv(buffer) };

    return input.tryRead(wrab.buffer.get().begin(), 1, size)
        .then([this, &input, completed, remaining, size, wrab = kj::mv(wrab)]
              (size_t actual) mutable -> kj::Promise<uint64_t> {
      if (actual == 0) {
        return completed;
      } if (actual < size) {
        wrab.buffer.truncate(actual);
      }

      wrab.request.adoptBytes(kj::mv(wrab.buffer));
      return wrab.request.send().ignoreResult().then([this, &input, completed, remaining, actual]() {
        return pumpLoop(input, completed + actual, remaining - actual);
      });
    });
  }
};

// =======================================================================================

class HttpOverCapnpFactory::CapnpToKjWebSocketAdapter final: public capnp::WebSocket::Server {
public:
  CapnpToKjWebSocketAdapter(kj::Own<RequestState> requestState)
      : requestState(kj::mv(requestState)) {}

  ~CapnpToKjWebSocketAdapter() noexcept(false) {
    KJ_IF_MAYBE(f, requestState->doneFulfiller) {
      f->fulfill();
    }
  }

  kj::Promise<void> sendText(SendTextContext context) override {
    auto& ws = *KJ_REQUIRE_NONNULL(
        requestState->assertState<kj::Own<kj::WebSocket>>(),
        "already called close()");
    return requestState->canceler.wrap(ws.send(context.getParams().getText()));
  }
  kj::Promise<void> sendData(SendDataContext context) override {
    auto& ws = *KJ_REQUIRE_NONNULL(
        requestState->assertState<kj::Own<kj::WebSocket>>(),
        "already called close()");
    return requestState->canceler.wrap(ws.send(context.getParams().getData()));
  }
  kj::Promise<void> close(CloseContext context) override {
    auto& ws = *KJ_REQUIRE_NONNULL(
        requestState->assertState<kj::Own<kj::WebSocket>>(),
        "already called close()");
    auto params = context.getParams();
    return requestState->canceler.wrap(ws.close(params.getCode(), params.getReason()));
  }

private:
  kj::Own<RequestState> requestState;
};

class HttpOverCapnpFactory::KjToCapnpWebSocketAdapter final: public kj::WebSocket {
public:
  KjToCapnpWebSocketAdapter(kj::Maybe<kj::Own<kj::WebSocket>> in, capnp::WebSocket::Client out)
      : in(kj::mv(in)), out(kj::mv(out)) {}

  kj::Promise<void> send(kj::ArrayPtr<const byte> message) override {
    auto req = KJ_REQUIRE_NONNULL(out, "already called disconnect()").sendDataRequest(
        MessageSize { 8 + message.size() / sizeof(word), 0 });
    req.setData(message);
    return req.send().ignoreResult();
  }

  kj::Promise<void> send(kj::ArrayPtr<const char> message) override {
    auto req = KJ_REQUIRE_NONNULL(out, "already called disconnect()").sendTextRequest(
        MessageSize { 8 + message.size() / sizeof(word), 0 });
    memcpy(req.initText(message.size()).begin(), message.begin(), message.size());
    return req.send().ignoreResult();
  }

  kj::Promise<void> close(uint16_t code, kj::StringPtr reason) override {
    auto req = KJ_REQUIRE_NONNULL(out, "already called disconnect()").closeRequest();
    req.setCode(code);
    req.setReason(reason);
    return req.send().ignoreResult();
  }

  kj::Promise<void> disconnect() override {
    out = nullptr;
    return kj::READY_NOW;
  }

  void abort() override {
    KJ_ASSERT_NONNULL(in)->abort();
  }

  kj::Promise<void> whenAborted() override {
    return KJ_ASSERT_NONNULL(in)->whenAborted();
  }

  kj::Promise<Message> receive() override {
    return KJ_ASSERT_NONNULL(in)->receive();
  }

  kj::Promise<void> pumpTo(WebSocket& other) override {
    // TODO(perf): Path-shorten if `other` is a `KjToCapnpWebSocketAdapter`.
    return KJ_ASSERT_NONNULL(in)->pumpTo(other);
  }

private:
  kj::Maybe<kj::Own<kj::WebSocket>> in;   // One end of a WebSocketPipe, used only for receiving.
  kj::Maybe<capnp::WebSocket::Client> out;  // Used only for sending.
};

// =======================================================================================

class HttpOverCapnpFactory::ClientRequestContextImpl final
    : public capnp::HttpService::ClientRequestContext::Server {
public:
  ClientRequestContextImpl(HttpOverCapnpFactory& factory,
                           kj::Own<RequestState> requestState)
      : factory(factory), requestState(kj::mv(requestState)) {}

  ~ClientRequestContextImpl() noexcept(false) {
    KJ_IF_MAYBE(f, requestState->doneFulfiller) {
      if (requestState->state.is<kj::HttpService::Response*>()) {
        f->reject(KJ_EXCEPTION(DISCONNECTED, "HTTP-over-capnp server never sent response"));
      }
    }
  }

  kj::Promise<void> startResponse(StartResponseContext context) override {
    auto& kjResponse = *KJ_REQUIRE_NONNULL(
        requestState->assertState<kj::HttpService::Response*>(),
        "already started response");

    auto params = context.getParams();
    auto rpcResponse = params.getResponse();

    auto bodySize = rpcResponse.getBodySize();
    kj::Maybe<uint64_t> expectedSize;
    bool hasBody = true;
    if (bodySize.isFixed()) {
      auto size = bodySize.getFixed();
      expectedSize = bodySize.getFixed();
      hasBody = size > 0;
    }

    auto bodyStream = kjResponse.send(rpcResponse.getStatusCode(), rpcResponse.getStatusText(),
        factory.headersToKj(rpcResponse.getHeaders()), expectedSize);

    auto results = context.getResults(MessageSize { 16, 1 });
    if (hasBody) {
      requestState->state = kj::mv(bodyStream);
      results.setBody(factory.streamSet.add(
          kj::heap<CapnpToKjStreamAdapter>(kj::addRef(*requestState))));
    } else {
      requestState->state = RequestState::Done();
      KJ_IF_MAYBE(df, requestState->doneFulfiller) {
        df->fulfill();
      }
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> startWebSocket(StartWebSocketContext context) override {
    auto& kjResponse = *KJ_REQUIRE_NONNULL(
        requestState->assertState<kj::HttpService::Response*>(),
        "already started response");

    auto params = context.getParams();

    auto webSocket = kjResponse.acceptWebSocket(factory.headersToKj(params.getHeaders()));
    auto upWrapper = kj::heap<KjToCapnpWebSocketAdapter>(nullptr, params.getUpSocket());
    setPumpTask(requestState->canceler.wrap(webSocket->pumpTo(*upWrapper).attach(kj::mv(upWrapper))
        .catch_([&ws=*webSocket](kj::Exception&& e) -> kj::Promise<void> {
      ws.abort();
      return kj::mv(e);
    })));
    requestState->state = kj::mv(webSocket);

    auto results = context.getResults(MessageSize { 16, 1 });
    results.setDownSocket(kj::heap<CapnpToKjWebSocketAdapter>(kj::addRef(*requestState)));

    return kj::READY_NOW;
  }

  void setPumpTask(kj::Promise<void> task) {
    pumpTask = task.eagerlyEvaluate([this](kj::Exception&& exception) {
      // TODO(cleanup): This feels like a hacky way to propagate this exception.
      KJ_IF_MAYBE(df, requestState->doneFulfiller) {
        df->reject(kj::mv(exception));
      }
    });
  }

private:
  HttpOverCapnpFactory& factory;
  kj::Own<RequestState> requestState;
  kj::Promise<void> pumpTask = nullptr;
};

class HttpOverCapnpFactory::ClientController {
public:
  ClientController(kj::PromiseFulfiller<void>& doneFulfiller,
                   HttpOverCapnpFactory& factory,
                   Request<capnp::HttpService::StartRequestParams,
                           capnp::HttpService::StartRequestResults>&& rpcRequest,
                   kj::Maybe<kj::AsyncInputStream&> requestBody,
                   kj::HttpService::Response& kjResponse)
      : state(kj::refcounted<RequestState>(doneFulfiller, kjResponse)),
        pumpRequestTask(nullptr), serverContext(nullptr) {
    auto clientRequestContext = kj::heap<ClientRequestContextImpl>(factory, kj::addRef(*state));
    auto& clientRequestContextRef = *clientRequestContext;
    capnp::HttpService::ClientRequestContext::Client cap = kj::mv(clientRequestContext);
    rpcRequest.setContext(cap);

    auto pipeline = rpcRequest.send();

    // Pump upstream -- unless we don't expect a request body.
    KJ_IF_MAYBE(rb, requestBody) {
      auto bodyOut = kj::heap<KjToCapnpStreamAdapter>(factory, pipeline.getRequestBody());
      clientRequestContextRef.setPumpTask(
          rb->pumpTo(*bodyOut).attach(kj::mv(bodyOut)).ignoreResult());
    }

    // Hold on to the ServerRequestContext so that we'll naturally cancel the request if the
    // ClientController is destroyed.
    serverContext = pipeline.getContext();
  }

  ~ClientController() noexcept(false) {
    state->doneFulfiller = nullptr;
    if (!state->canceler.isEmpty()) {
      state->canceler.cancel(KJ_EXCEPTION(DISCONNECTED, "request canceled"));
    }
    state->state = RequestState::Canceled();
  }

private:
  kj::Own<RequestState> state;
  kj::Promise<uint64_t> pumpRequestTask;
  capnp::HttpService::ServerRequestContext::Client serverContext;
};

class HttpOverCapnpFactory::KjToCapnpHttpServiceAdapter final: public kj::HttpService {
public:
  KjToCapnpHttpServiceAdapter(HttpOverCapnpFactory& factory, capnp::HttpService::Client inner)
      : factory(factory), inner(kj::mv(inner)) {}

  kj::Promise<void> request(
      kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody, kj::HttpService::Response& response) override {
    auto rpcRequest = inner.startRequestRequest();

    auto metadata = rpcRequest.initRequest();
    metadata.setMethod(static_cast<capnp::HttpMethod>(method));
    metadata.setUrl(url);
    metadata.adoptHeaders(factory.headersToCapnp(
        headers, Orphanage::getForMessageContaining(metadata)));

    kj::Maybe<kj::AsyncInputStream&> maybeRequestBody;

    if (method == kj::HttpMethod::GET || method == kj::HttpMethod::HEAD) {
      maybeRequestBody = nullptr;
      metadata.getBodySize().setFixed(0);
    } else KJ_IF_MAYBE(s, requestBody.tryGetLength()) {
      metadata.getBodySize().setFixed(*s);
      if (*s == 0) {
        maybeRequestBody = nullptr;
      } else {
        maybeRequestBody = requestBody;
      }
    } else {
      metadata.getBodySize().setUnknown();
      maybeRequestBody = requestBody;
    }

    return kj::newAdaptedPromise<void, ClientController>(
        factory, kj::mv(rpcRequest), maybeRequestBody, response);
  }

private:
  HttpOverCapnpFactory& factory;
  capnp::HttpService::Client inner;
};

kj::Own<kj::HttpService> HttpOverCapnpFactory::capnpToKj(capnp::HttpService::Client rpcService) {
  return kj::heap<KjToCapnpHttpServiceAdapter>(*this, kj::mv(rpcService));
}

// =======================================================================================

namespace {

class NullInputStream final: public kj::AsyncInputStream {
  // TODO(cleanup): This class has been replicated in a bunch of places now, make it public
  //   somewhere.

public:
  kj::Promise<size_t> tryRead(void* buffer, size_t minBytes, size_t maxBytes) override {
    return size_t(0);
  }

  kj::Maybe<uint64_t> tryGetLength() override {
    return uint64_t(0);
  }

  kj::Promise<uint64_t> pumpTo(kj::AsyncOutputStream& output, uint64_t amount) override {
    return uint64_t(0);
  }
};

class NullOutputStream final: public kj::AsyncOutputStream {
  // TODO(cleanup): This class has been replicated in a bunch of places now, make it public
  //   somewhere.

public:
  kj::Promise<void> write(const void* buffer, size_t size) override {
    return kj::READY_NOW;
  }
  kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const byte>> pieces) override {
    return kj::READY_NOW;
  }
  kj::Promise<void> whenWriteDisconnected() override {
    return kj::NEVER_DONE;
  }

  // We can't really optimize tryPumpFrom() unless AsyncInputStream grows a skip() method.
};

}  // namespace

class HttpOverCapnpFactory::ServerRequestContextImpl final
    : public capnp::HttpService::ServerRequestContext::Server,
      public kj::HttpService::Response {
public:
  ServerRequestContextImpl(HttpOverCapnpFactory& factory,
                           kj::Own<RequestState> state,
                           capnp::HttpRequest::Reader request,
                           capnp::HttpService::ClientRequestContext::Client clientContext,
                           kj::Own<kj::AsyncInputStream> requestBodyInParam,
                           kj::HttpService& kjService)
      : factory(factory),
        method(validateMethod(request.getMethod())),
        headers(factory.headersToKj(request.getHeaders())),
        state(kj::mv(state)),
        clientContext(kj::mv(clientContext)),
        requestBodyIn(kj::mv(requestBodyInParam)),
        task(kjService.request(method, request.getUrl(), headers, *requestBodyIn, *this)
            .eagerlyEvaluate([this](kj::Exception&& exception) {
          this->state->canceler.cancel(kj::mv(exception));
        })) {}

  ~ServerRequestContextImpl() noexcept(false) {
    if (!state->canceler.isEmpty()) {
      state->canceler.cancel(KJ_EXCEPTION(DISCONNECTED, "request canceled"));
    }
    state->state = RequestState::Canceled();
  }

  kj::Own<kj::AsyncOutputStream> send(
      uint statusCode, kj::StringPtr statusText, const kj::HttpHeaders& headers,
      kj::Maybe<uint64_t> expectedBodySize = nullptr) override {
    auto req = clientContext.startResponseRequest();

    if (method == kj::HttpMethod::HEAD ||
        statusCode == 204 || statusCode == 205 || statusCode == 304) {
      expectedBodySize = uint64_t(0);
    }

    auto rpcResponse = req.initResponse();
    rpcResponse.setStatusCode(statusCode);
    rpcResponse.setStatusText(statusText);
    rpcResponse.adoptHeaders(factory.headersToCapnp(
        headers, Orphanage::getForMessageContaining(rpcResponse)));
    bool hasBody = true;
    KJ_IF_MAYBE(s, expectedBodySize) {
      rpcResponse.getBodySize().setFixed(*s);
      hasBody = *s > 0;
    }

    if (hasBody) {
      auto pipeline = req.send();
      auto result = kj::heap<KjToCapnpStreamAdapter>(factory, pipeline.getBody());
      setReplyTask(pipeline.ignoreResult());
      return result;
    } else {
      setReplyTask(req.send().ignoreResult());
      return kj::heap<NullOutputStream>();
    }
  }

  kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders& headers) override {
    auto req = clientContext.startWebSocketRequest();

    req.adoptHeaders(factory.headersToCapnp(
        headers, Orphanage::getForMessageContaining(
            capnp::HttpService::ClientRequestContext::StartWebSocketParams::Builder(req))));

    auto pipe = kj::newWebSocketPipe();
    req.setUpSocket(kj::heap<CapnpToKjWebSocketAdapter>(
        kj::heap<RequestState>(kj::mv(pipe.ends[0]))));
    auto pipeline = req.send();
    auto result = kj::heap<KjToCapnpWebSocketAdapter>(
        kj::mv(pipe.ends[1]), pipeline.getDownSocket());
    setReplyTask(pipeline.ignoreResult());
    return result;
  }

private:
  HttpOverCapnpFactory& factory;
  kj::HttpMethod method;
  kj::HttpHeaders headers;
  kj::Own<RequestState> state;
  capnp::HttpService::ClientRequestContext::Client clientContext;
  kj::Own<kj::AsyncInputStream> requestBodyIn;
  kj::Promise<void> task;
  kj::Promise<void> replyTask = nullptr;

  static kj::HttpMethod validateMethod(capnp::HttpMethod method) {
    KJ_REQUIRE(method <= capnp::HttpMethod::UNSUBSCRIBE, "unknown method", method);
    return static_cast<kj::HttpMethod>(method);
  }

  void setReplyTask(kj::Promise<void> promise) {
    replyTask = promise.eagerlyEvaluate([this](kj::Exception&& exception) {
      state->canceler.cancel(kj::mv(exception));
    });
  }
};

class HttpOverCapnpFactory::CapnpToKjHttpServiceAdapter final: public capnp::HttpService::Server {
public:
  CapnpToKjHttpServiceAdapter(HttpOverCapnpFactory& factory, kj::Own<kj::HttpService> inner)
      : factory(factory), inner(kj::mv(inner)) {}

  kj::Promise<void> startRequest(StartRequestContext context) override {
    auto params = context.getParams();
    auto metadata = params.getRequest();

    auto bodySize = metadata.getBodySize();
    kj::Maybe<uint64_t> expectedSize;
    bool hasBody = true;
    if (bodySize.isFixed()) {
      auto size = bodySize.getFixed();
      expectedSize = bodySize.getFixed();
      hasBody = size > 0;
    }

    auto results = context.getResults(MessageSize {8, 2});
    kj::Own<RequestState> state;
    kj::Own<kj::AsyncInputStream> requestBody;
    if (hasBody) {
      auto pipe = kj::newOneWayPipe(expectedSize);
      state = kj::refcounted<RequestState>(kj::mv(pipe.out));
      results.setRequestBody(kj::heap<CapnpToKjStreamAdapter>(kj::addRef(*state)));
      requestBody = kj::mv(pipe.in);
    } else {
      state = kj::refcounted<RequestState>();
      requestBody = kj::heap<NullInputStream>();
    }
    results.setContext(kj::heap<ServerRequestContextImpl>(
        factory, kj::mv(state), metadata, params.getContext(), kj::mv(requestBody), *inner));

    return kj::READY_NOW;
  }

private:
  HttpOverCapnpFactory& factory;
  kj::Own<kj::HttpService> inner;
};

capnp::HttpService::Client HttpOverCapnpFactory::kjToCapnp(kj::Own<kj::HttpService> service) {
  return kj::heap<CapnpToKjHttpServiceAdapter>(*this, kj::mv(service));
}

// =======================================================================================

static constexpr uint64_t COMMON_TEXT_ANNOTATION = 0x857745131db6fc83ull;
// Type ID of `commonText` from `http.capnp`.
// TODO(cleanup): Cap'n Proto should auto-generate constants for these.

HttpOverCapnpFactory::HttpOverCapnpFactory(kj::HttpHeaderTable::Builder& headerTableBuilder)
    : headerTable(headerTableBuilder.getFutureTable()) {
  auto commonHeaderNames = Schema::from<capnp::CommonHeaderName>().getEnumerants();
  size_t maxHeaderId = 0;
  nameCapnpToKj = kj::heapArray<kj::HttpHeaderId>(commonHeaderNames.size());
  for (size_t i = 1; i < commonHeaderNames.size(); i++) {
    kj::StringPtr nameText;
    for (auto ann: commonHeaderNames[i].getProto().getAnnotations()) {
      if (ann.getId() == COMMON_TEXT_ANNOTATION) {
        nameText = ann.getValue().getText();
        break;
      }
    }
    KJ_ASSERT(nameText != nullptr);
    kj::HttpHeaderId headerId = headerTableBuilder.add(nameText);
    nameCapnpToKj[i] = headerId;
    maxHeaderId = kj::max(maxHeaderId, headerId.hashCode());
  }

  nameKjToCapnp = kj::heapArray<capnp::CommonHeaderName>(maxHeaderId + 1);
  for (auto& slot: nameKjToCapnp) slot = capnp::CommonHeaderName::INVALID;

  for (size_t i = 1; i < commonHeaderNames.size(); i++) {
    auto& slot = nameKjToCapnp[nameCapnpToKj[i].hashCode()];
    KJ_ASSERT(slot == capnp::CommonHeaderName::INVALID);
    slot = static_cast<capnp::CommonHeaderName>(i);
  }

  auto commonHeaderValues = Schema::from<capnp::CommonHeaderValue>().getEnumerants();
  valueCapnpToKj = kj::heapArray<kj::StringPtr>(commonHeaderValues.size());
  for (size_t i = 1; i < commonHeaderValues.size(); i++) {
    kj::StringPtr valueText;
    for (auto ann: commonHeaderValues[i].getProto().getAnnotations()) {
      if (ann.getId() == COMMON_TEXT_ANNOTATION) {
        valueText = ann.getValue().getText();
        break;
      }
    }
    KJ_ASSERT(valueText != nullptr);
    valueCapnpToKj[i] = valueText;
    valueKjToCapnp.insert(valueText, static_cast<capnp::CommonHeaderValue>(i));
  }
}

Orphan<List<capnp::HttpHeader>> HttpOverCapnpFactory::headersToCapnp(
    const kj::HttpHeaders& headers, Orphanage orphanage) {
  auto result = orphanage.newOrphan<List<capnp::HttpHeader>>(headers.size());
  auto rpcHeaders = result.get();
  uint i = 0;
  headers.forEach([&](kj::HttpHeaderId id, kj::StringPtr value) {
    auto capnpName = nameKjToCapnp[id.hashCode()];
    if (capnpName == capnp::CommonHeaderName::INVALID) {
      auto header = rpcHeaders[i++].initUncommon();
      header.setName(id.toString());
      header.setValue(value);
    } else {
      auto header = rpcHeaders[i++].initCommon();
      header.setName(capnpName);
      header.setValue(value);
    }
  }, [&](kj::StringPtr name, kj::StringPtr value) {
    auto header = rpcHeaders[i++].initUncommon();
    header.setName(name);
    header.setValue(value);
  });
  KJ_ASSERT(i == rpcHeaders.size());
  return result;
}

kj::HttpHeaders HttpOverCapnpFactory::headersToKj(
    List<capnp::HttpHeader>::Reader capnpHeaders) const {
  kj::HttpHeaders result(headerTable);

  for (auto header: capnpHeaders) {
    switch (header.which()) {
      case capnp::HttpHeader::COMMON: {
        auto nv = header.getCommon();
        auto nameInt = static_cast<uint>(nv.getName());
        KJ_REQUIRE(nameInt < nameCapnpToKj.size(), "unknown common header name", nv.getName());

        switch (nv.which()) {
          case capnp::HttpHeader::Common::COMMON_VALUE: {
            auto cvInt = static_cast<uint>(nv.getCommonValue());
            KJ_REQUIRE(nameInt < valueCapnpToKj.size(),
                "unknown common header value", nv.getCommonValue());
            result.set(nameCapnpToKj[nameInt], valueCapnpToKj[cvInt]);
            break;
          }
          case capnp::HttpHeader::Common::VALUE:
            result.set(nameCapnpToKj[nameInt], nv.getValue());
            break;
        }
        break;
      }
      case capnp::HttpHeader::UNCOMMON: {
        auto nv = header.getUncommon();
        result.add(nv.getName(), nv.getValue());
      }
    }
  }

  return result;
}

}  // namespace capnp
