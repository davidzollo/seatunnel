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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reconnection strategy with exponential backoff
 *
 * <p>Manages reconnection attempts and calculates backoff delays
 */
public class PIWebSocketReconnectStrategy {

    private final int maxReconnectAttempts;
    private final long backoffInitialMillis;
    private final long backoffMaxMillis;
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);

    public PIWebSocketReconnectStrategy(
            int maxReconnectAttempts, long backoffInitialMillis, long backoffMaxMillis) {
        this.maxReconnectAttempts = maxReconnectAttempts;
        this.backoffInitialMillis = backoffInitialMillis;
        this.backoffMaxMillis = backoffMaxMillis;
    }

    /**
     * Check if can retry for given attempt number
     *
     * @param attempt Current attempt number
     * @return true if can retry
     */
    public boolean canRetry(int attempt) {
        return maxReconnectAttempts <= 0 || attempt < maxReconnectAttempts;
    }

    /**
     * Calculate backoff delay for given attempt
     *
     * @param attempt Current attempt number (0-based)
     * @return Delay in milliseconds
     */
    public long calculateBackoffDelayMs(int attempt) {
        if (attempt <= 0) {
            return 0;
        }
        long multiplier = 1L << (attempt - 1);
        long delay = backoffInitialMillis * multiplier;
        if (delay < 0 || delay > Long.MAX_VALUE / 2) {
            delay = backoffMaxMillis;
        }
        return Math.min(delay, backoffMaxMillis);
    }

    /**
     * Set reconnect attempts count
     *
     * @param attempts New attempts count
     */
    public void setReconnectAttempts(int attempts) {
        reconnectAttempts.set(attempts);
    }

    /** Reset reconnect attempts counter to 0 */
    public void reset() {
        reconnectAttempts.set(0);
    }

    public int getMaxReconnectAttempts() {
        return maxReconnectAttempts;
    }
}
