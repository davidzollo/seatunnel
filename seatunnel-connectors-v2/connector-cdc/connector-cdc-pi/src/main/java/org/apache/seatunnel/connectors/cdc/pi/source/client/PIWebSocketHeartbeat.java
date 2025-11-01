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
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Manages WebSocket heartbeat
 *
 * <p>Handles periodic ping frame sending to keep connection alive
 */
public class PIWebSocketHeartbeat {

    private static final Logger log = LoggerFactory.getLogger(PIWebSocketHeartbeat.class);

    private final int heartbeatIntervalSeconds;
    private volatile ScheduledFuture<?> heartbeatTask;

    public PIWebSocketHeartbeat(int heartbeatIntervalSeconds) {
        this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
    }

    /**
     * Start heartbeat task
     *
     * @param channel WebSocket channel
     */
    public void start(Channel channel) {
        if (heartbeatIntervalSeconds <= 0 || channel == null || channel.eventLoop() == null) {
            return;
        }

        // Cancel existing heartbeat task and wait for completion to prevent double heartbeat
        stop();

        // Use channel.eventLoop() to bind heartbeat to the connection's EventLoop
        // This ensures heartbeat lifecycle is naturally tied to the channel
        heartbeatTask =
                channel.eventLoop()
                        .scheduleAtFixedRate(
                                () -> {
                                    if (channel.isActive()) {
                                        log.debug("Send heartbeat PING");
                                        channel.writeAndFlush(
                                                new PingWebSocketFrame(
                                                        Unpooled.wrappedBuffer(
                                                                new byte[] {1, 2, 3})));
                                    }
                                },
                                heartbeatIntervalSeconds,
                                heartbeatIntervalSeconds,
                                TimeUnit.SECONDS);
        log.debug("Started heartbeat task with {}s interval", heartbeatIntervalSeconds);
    }

    /** Stop heartbeat task */
    public void stop() {
        ScheduledFuture<?> future = heartbeatTask;
        heartbeatTask = null;
        if (future != null) {
            future.cancel(false);
            log.debug("Stopped heartbeat task");
        }
    }

    /**
     * Check if heartbeat is running
     *
     * @return true if heartbeat task is active
     */
    public boolean isRunning() {
        return heartbeatTask != null && !heartbeatTask.isCancelled() && !heartbeatTask.isDone();
    }
}
