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
package org.apache.seatunnel.engine.server.utils;

import org.apache.commons.lang3.StringUtils;

import com.squareup.okhttp.OkHttpClient;
import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLContext;

import java.util.concurrent.TimeUnit;

@Slf4j
public class HttpUtils {

    private static OkHttpClient httpClient;

    private HttpUtils() {}

    public static OkHttpClient getInstance() {
        return getInstance(null, null);
    }

    public static OkHttpClient getInstance(String keystorePath, String keystorePassword) {
        if (httpClient == null) {
            synchronized (HttpUtils.class) {
                if (httpClient == null) {
                    httpClient = createHttpClient(keystorePath, keystorePassword);
                }
            }
        }
        return httpClient;
    }

    public static OkHttpClient createHttpClient(String keystorePath, String keystorePassword) {
        OkHttpClient client = new OkHttpClient();
        client.setConnectTimeout(30, TimeUnit.SECONDS);
        client.setWriteTimeout(10, TimeUnit.SECONDS);

        if (StringUtils.isNotBlank(keystorePath) && StringUtils.isNotBlank(keystorePassword)) {
            try {
                SSLContext sslContext = SSLUtils.createSSLContext(keystorePath, keystorePassword);
                client.setSslSocketFactory(sslContext.getSocketFactory());
                log.info("HTTPS SSL context configured with keystore: {}", keystorePath);
            } catch (Exception e) {
                log.error(
                        "Failed to configure SSL context for HTTPS, falling back to default HTTP client",
                        e);
            }
        }

        return client;
    }
}
