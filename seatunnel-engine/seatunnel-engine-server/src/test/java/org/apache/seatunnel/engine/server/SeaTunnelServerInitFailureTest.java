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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.spi.impl.NodeEngineImpl;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

public class SeaTunnelServerInitFailureTest {

    private final SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();

    @Test
    public void shouldFailFastWhenInitThrowsError() {
        NoClassDefFoundError expectedFailure = new NoClassDefFoundError("missing-class");
        TestSeaTunnelServer server = new TestSeaTunnelServer(seaTunnelConfig, expectedFailure);

        ExitException exitException =
                Assertions.assertThrows(
                        ExitException.class, () -> server.init(null, new Properties()));

        Assertions.assertSame(expectedFailure, server.capturedFailure);
        Assertions.assertTrue(server.shutdownCalled);
        Assertions.assertEquals(1, exitException.statusCode);
    }

    @Test
    public void shouldRestoreOriginalContextClassLoaderWhenInitActionFails() throws Exception {
        SeaTunnelServer server = new SeaTunnelServer(seaTunnelConfig);
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        NoClassDefFoundError expectedFailure = new NoClassDefFoundError("missing-class");

        try (URLClassLoader testClassLoader = new URLClassLoader(new URL[0], originalClassLoader)) {
            NoClassDefFoundError actualFailure =
                    Assertions.assertThrows(
                            NoClassDefFoundError.class,
                            () ->
                                    server.runWithThreadContextClassLoader(
                                            testClassLoader,
                                            () -> {
                                                Assertions.assertSame(
                                                        testClassLoader,
                                                        Thread.currentThread()
                                                                .getContextClassLoader());
                                                throw expectedFailure;
                                            }));
            Assertions.assertSame(expectedFailure, actualFailure);
        }

        Assertions.assertSame(originalClassLoader, Thread.currentThread().getContextClassLoader());
    }

    private static final class TestSeaTunnelServer extends SeaTunnelServer {

        private final Throwable initFailureToThrow;
        private Throwable capturedFailure;
        private boolean shutdownCalled;

        private TestSeaTunnelServer(SeaTunnelConfig seaTunnelConfig, Throwable initFailureToThrow) {
            super(seaTunnelConfig);
            this.initFailureToThrow = initFailureToThrow;
        }

        @Override
        void initInternal(NodeEngineImpl engine) throws Throwable {
            throw initFailureToThrow;
        }

        @Override
        void handleInitializationFailure(Throwable throwable) {
            this.capturedFailure = throwable;
            super.handleInitializationFailure(throwable);
        }

        @Override
        public void shutdown(boolean terminate) {
            shutdownCalled = true;
        }

        @Override
        void exitProcess(int statusCode) {
            throw new ExitException(statusCode);
        }
    }

    private static final class ExitException extends RuntimeException {

        private final int statusCode;

        private ExitException(int statusCode) {
            this.statusCode = statusCode;
        }
    }
}
