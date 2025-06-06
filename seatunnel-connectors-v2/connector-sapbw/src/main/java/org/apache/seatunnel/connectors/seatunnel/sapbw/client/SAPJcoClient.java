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

package org.apache.seatunnel.connectors.seatunnel.sapbw.client;

import org.apache.seatunnel.connectors.seatunnel.sapbw.config.SAPBWSourceConfig;

import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.ext.DestinationDataProvider;
import com.sap.conn.jco.ext.Environment;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

@Slf4j
public class SAPJcoClient implements AutoCloseable {

    private final String dest;
    private volatile JCoDestination destination;
    private static final MemoryDestinationDataProvider provider;

    static {
        provider = new MemoryDestinationDataProvider();
        if (!Environment.isDestinationDataProviderRegistered()) {
            Environment.registerDestinationDataProvider(provider);
        }
    }

    public static SAPJcoClient createClient(SAPBWSourceConfig config) {
        Properties connectProperties = new Properties();
        connectProperties.setProperty(
                DestinationDataProvider.JCO_ASHOST, config.getApplicationServerHost());
        connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR, config.getSystemNumber());
        connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT, config.getClient());
        connectProperties.setProperty(DestinationDataProvider.JCO_USER, config.getUser());
        connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD, config.getPassword());
        connectProperties.setProperty(DestinationDataProvider.JCO_LANG, config.getLanguage());
        String destName =
                config.getApplicationServerHost()
                        + "_"
                        + config.getSystemNumber()
                        + "_"
                        + config.getClient()
                        + "_"
                        + config.getUser()
                        + "_"
                        + config.getLanguage();
        return new SAPJcoClient(destName, connectProperties);
    }

    private SAPJcoClient(String dest, Properties connectProperties) {
        this.dest = dest;
        provider.addDestination(dest, connectProperties);
    }

    public synchronized JCoDestination getDestination() {
        if (destination == null) {
            try {
                destination = JCoDestinationManager.getDestination(dest);
                log.info("New connected to: {}", destination.getAttributes().getHost());
            } catch (Exception e) {
                throw new RuntimeException("Failed to get SAP destination", e);
            }
        }
        return destination;
    }

    @Override
    public void close() throws Exception {
        if (destination != null) {
            log.info("Disconnected from SAP destination {}", destination.getAttributes().getHost());
            destination = null;
        }
    }
}
