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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages WebSocket connection state
 *
 * <p>Thread-safe state management for WebSocket client
 */
public class PIWebSocketConnectionState {

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean connecting = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private final AtomicReference<String> lastError = new AtomicReference<>();

    private volatile long lastConnectAttemptTime = 0;
    private volatile long connectionEstablishedTime = 0;

    public boolean isRunning() {
        return running.get();
    }

    public boolean isConnecting() {
        return connecting.get();
    }

    public boolean isConnected() {
        return connected.get();
    }

    public boolean isReconnectScheduled() {
        return reconnectScheduled.get();
    }

    public String getLastError() {
        return lastError.get();
    }

    public long getLastConnectAttemptTime() {
        return lastConnectAttemptTime;
    }

    public long getConnectionEstablishedTime() {
        return connectionEstablishedTime;
    }

    public boolean startRunning() {
        return running.compareAndSet(false, true);
    }

    public boolean stopRunning() {
        return running.compareAndSet(true, false);
    }

    public boolean startConnecting() {
        return connecting.compareAndSet(false, true);
    }

    public boolean stopConnecting() {
        return connecting.compareAndSet(true, false);
    }

    public void setConnecting(boolean value) {
        connecting.set(value);
    }

    public void setConnected(boolean value) {
        connected.set(value);
    }

    public boolean getAndSetConnected(boolean value) {
        return connected.getAndSet(value);
    }

    public boolean scheduleReconnect() {
        return reconnectScheduled.compareAndSet(false, true);
    }

    public void clearReconnectScheduled() {
        reconnectScheduled.set(false);
    }

    public void setLastError(String error) {
        lastError.set(error);
    }

    public void clearLastError() {
        lastError.set(null);
    }

    public void updateLastConnectAttemptTime() {
        this.lastConnectAttemptTime = System.currentTimeMillis();
    }

    public void updateConnectionEstablishedTime() {
        this.connectionEstablishedTime = System.currentTimeMillis();
    }

    public void reset() {
        connected.set(false);
        connecting.set(false);
        reconnectScheduled.set(false);
    }

    /**
     * Get connection status description
     *
     * @return Connection status string
     */
    public String getStatusDescription() {
        if (connected.get()) {
            long uptime = System.currentTimeMillis() - connectionEstablishedTime;
            return String.format("Connected (uptime: %dms)", uptime);
        } else if (connecting.get()) {
            long connectingTime = System.currentTimeMillis() - lastConnectAttemptTime;
            return String.format("Connecting (attempted: %dms)", connectingTime);
        } else {
            String error = lastError.get();
            return "Not connected" + (error != null ? " (error: " + error + ")" : "");
        }
    }
}
