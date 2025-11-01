/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.connectors.cdc.pi.source.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.CharsetUtil;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * WebSocket message handler
 *
 * <p>Handles WebSocket handshake and frame processing
 */
public class PIWebSocketHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger log = LoggerFactory.getLogger(PIWebSocketHandler.class);

    private final WebSocketClientHandshaker handshaker;
    private final PIWebSocketConnectionState connectionState;
    private final PIWebSocketReconnectStrategy reconnectStrategy;
    private final PIWebSocketHeartbeat heartbeat;
    private final CountDownLatch shutdownLatch;
    private final Runnable reconnectCallback;

    private ChannelPromise handshakeFuture;

    private volatile Consumer<String> onMessage;
    private volatile Runnable onOpen;
    private volatile Consumer<CloseWebSocketFrame> onClose;

    public PIWebSocketHandler(
            WebSocketClientHandshaker handshaker,
            PIWebSocketConnectionState connectionState,
            PIWebSocketReconnectStrategy reconnectStrategy,
            PIWebSocketHeartbeat heartbeat,
            CountDownLatch shutdownLatch,
            Runnable reconnectCallback) {
        this.handshaker = handshaker;
        this.connectionState = connectionState;
        this.reconnectStrategy = reconnectStrategy;
        this.heartbeat = heartbeat;
        this.shutdownLatch = shutdownLatch;
        this.reconnectCallback = reconnectCallback;
    }

    public void setOnMessage(Consumer<String> onMessage) {
        this.onMessage = onMessage;
    }

    public void setOnOpen(Runnable onOpen) {
        this.onOpen = onOpen;
    }

    public void setOnClose(Consumer<CloseWebSocketFrame> onClose) {
        this.onClose = onClose;
    }

    public ChannelFuture handshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        log.debug("WebSocket handler added to Pipeline");
        handshakeFuture = ctx.newPromise();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.debug("Channel activated, start WebSocket handshake");
        try {
            handshaker.handshake(ctx.channel());
            log.debug("WebSocket handshake request sent");
        } catch (Exception e) {
            log.error("WebSocket handshake request sending failed", e);
            throw e;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.warn("WebSocket connection disconnected");

        boolean wasConnected = connectionState.getAndSetConnected(false);

        // Cancel heartbeat task immediately when connection drops
        heartbeat.stop();

        if (onClose != null) {
            try {
                CloseWebSocketFrame closeFrame =
                        new CloseWebSocketFrame(1000, "Connection disconnected");
                onClose.accept(closeFrame);
            } catch (Exception e) {
                log.error("Exception occurred while executing onClose callback", e);
                try {
                    onClose.accept(null);
                } catch (Exception ex) {
                    log.error("Exception occurred while executing onClose callback", ex);
                }
            }
        }

        // Only reconnect if connection was previously established
        if (connectionState.isRunning() && wasConnected && !connectionState.isConnecting()) {
            log.warn(
                    "Connection was established but now disconnected, scheduling reconnect from attempt 0");
            reconnectStrategy.reset();
            if (reconnectCallback != null) {
                reconnectCallback.run();
            }
        } else if (!wasConnected) {
            log.debug(
                    "Connection never established, skip scheduling reconnect (connectWithRetry will handle it)");
        } else if (!connectionState.isRunning()) {
            shutdownLatch.countDown();
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent idleEvent = (IdleStateEvent) evt;
            log.debug("Detected idle state: {}", idleEvent.state());
            if (ctx.channel().isActive()) {
                ctx.writeAndFlush(
                        new PingWebSocketFrame(Unpooled.wrappedBuffer(new byte[] {1, 2, 3})));
            }
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        Channel ch = ctx.channel();

        // Handle handshake completion
        if (!handshaker.isHandshakeComplete()) {
            log.debug("Handle WebSocket handshake response");
            try {
                FullHttpResponse response = (FullHttpResponse) msg;
                log.debug("Received handshake response, status code: {}", response.status());
                handshaker.finishHandshake(ch, response);
                handshakeFuture.setSuccess();
                log.debug("WebSocket handshake completed successfully");

                // Trigger connection success callback
                connectionState.setConnected(true);
                connectionState.updateConnectionEstablishedTime();
                reconnectStrategy.reset();

                if (onOpen != null) {
                    try {
                        onOpen.run();
                    } catch (Exception e) {
                        log.error("Exception occurred while executing onOpen callback", e);
                    }
                }
            } catch (Exception e) {
                log.error("WebSocket handshake failed", e);
                handshakeFuture.setFailure(e);
            }
            return;
        }

        // Handle unexpected HTTP response
        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;
            throw new IllegalStateException(
                    "Unexpected FullHttpResponse (getStatus="
                            + response.status()
                            + ", content="
                            + response.content().toString(CharsetUtil.UTF_8)
                            + ')');
        }

        // Handle WebSocket frame
        if (msg instanceof WebSocketFrame) {
            WebSocketFrame frame = (WebSocketFrame) msg;
            if (frame instanceof TextWebSocketFrame) {
                handleTextFrame((TextWebSocketFrame) frame);
            } else if (frame instanceof BinaryWebSocketFrame) {
                handleBinaryFrame((BinaryWebSocketFrame) frame);
            } else if (frame instanceof PingWebSocketFrame) {
                handlePingFrame(ctx, (PingWebSocketFrame) frame);
            } else if (frame instanceof PongWebSocketFrame) {
                handlePongFrame();
            } else if (frame instanceof CloseWebSocketFrame) {
                handleCloseFrame(ch, (CloseWebSocketFrame) frame);
            }
        }
    }

    private void handleTextFrame(TextWebSocketFrame frame) {
        String text = frame.text();
        if (onMessage != null) {
            try {
                onMessage.accept(text);
            } catch (Exception e) {
                log.error("Exception occurred while executing onMessage callback", e);
            }
        }
    }

    private void handleBinaryFrame(BinaryWebSocketFrame frame) {
        log.debug("Received binary message, length: {}", frame.content().readableBytes());
    }

    private void handlePingFrame(ChannelHandlerContext ctx, PingWebSocketFrame frame) {
        log.debug("Received PING");
        ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
    }

    private void handlePongFrame() {
        log.debug("Received PONG heartbeat response");
    }

    private void handleCloseFrame(Channel ch, CloseWebSocketFrame frame) {
        log.debug("Received close frame");
        if (onClose != null) {
            try {
                onClose.accept(frame);
            } catch (Exception e) {
                log.error("Exception occurred while executing onClose callback", e);
            }
        }
        ch.close();
        shutdownLatch.countDown();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }

        log.error("WebSocket connection exception occurred: {}", cause.getMessage(), cause);
        logExceptionDetails(cause);
        ctx.close();
    }

    private void logExceptionDetails(Throwable cause) {
        if (cause instanceof javax.net.ssl.SSLHandshakeException) {
            log.error("SSL handshake failed: {}", cause.getMessage());
            log.error(
                    "Possible reasons: 1) TLS version mismatch 2) Certificate problem 3) Password suite incompatibility");
        } else if (cause instanceof javax.net.ssl.SSLException) {
            log.error("SSL exception: {}", cause.getMessage());
            log.error("Possible reasons: 1) SSL connection error occurred during the process");
        } else if (cause instanceof java.net.ConnectException) {
            log.error("Connection refused: {}", cause.getMessage());
            log.error(
                    "Possible reasons: 1) Server not started 2) Port occupied 3) Firewall blocked");
        } else if (cause instanceof java.net.SocketTimeoutException) {
            log.error("Connection timeout: {}", cause.getMessage());
            log.error(
                    "Possible reasons: 1) Network delay too high 2) Server response slow 3) Connection timeout set too short");
        } else if (cause instanceof java.io.IOException) {
            log.error("IO exception: {}", cause.getMessage());
            log.error(
                    "Possible reasons: 1) Network interruption 2) Connection reset 3) Data transmission error");
        } else if (cause instanceof io.netty.handler.timeout.TimeoutException) {
            log.error("Netty timeout exception: {}", cause.getMessage());
            log.error(
                    "Possible reasons: 1) Handshake timeout 2) Read/write timeout 3) Heartbeat timeout");
        } else if (cause instanceof java.security.cert.CertificateException) {
            log.error("Certificate exception: {}", cause.getMessage());
            log.error(
                    "Possible reasons: 1) Certificate invalid 2) Certificate expired 3) Certificate chain verification failed");
        } else {
            log.error(
                    "Unknown exception type: {} - {}",
                    cause.getClass().getSimpleName(),
                    cause.getMessage());
        }
    }
}
