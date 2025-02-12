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

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopLoginFactory;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.config.HdfsConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.HiveSinkOptions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Objects;

@Slf4j
public class HiveMetaStoreProxy {
    private HiveMetaStoreClient hiveMetaStoreClient;
    private static volatile HiveMetaStoreProxy INSTANCE = null;

    private HiveMetaStoreProxy(ReadonlyConfig readonlyConfig) {
        String metastoreUri = readonlyConfig.get(HiveSinkOptions.METASTORE_URI);

        try {
            HiveConf hiveConf = new HiveConf();
            hiveConf.set("hive.metastore.uris", metastoreUri);
            readonlyConfig
                    .getOptional(HiveSinkOptions.HIVE_SITE_PATH)
                    .ifPresent(
                            hiveSite -> {
                                try {
                                    if (StringUtils.isNotBlank(hiveSite)) {
                                        hiveConf.addResource(new File(hiveSite).toURI().toURL());
                                    }
                                } catch (MalformedURLException e) {
                                    throw new RuntimeException(
                                            "Add Hive site failed: " + hiveSite, e);
                                }
                            });
            if (enableKerberos(readonlyConfig)) {
                Configuration configuration = new Configuration();
                configuration.set("hadoop.security.authentication", "kerberos");
                this.hiveMetaStoreClient =
                        HadoopLoginFactory.loginWithKerberos(
                                configuration,
                                readonlyConfig.get(HdfsConfigOptions.KRB5_PATH),
                                readonlyConfig.get(HdfsConfigOptions.KERBEROS_PRINCIPAL),
                                readonlyConfig.get(HdfsConfigOptions.KERBEROS_KEYTAB_PATH),
                                (conf, userGroupInformation) -> new HiveMetaStoreClient(hiveConf));
                log.info(
                        "Create HiveMetaStoreClient success with Kerberos: {}",
                        readonlyConfig.get(HdfsConfigOptions.REMOTE_USER));
                return;
            }
            if (enableRemoteUser(readonlyConfig)) {
                this.hiveMetaStoreClient =
                        HadoopLoginFactory.loginWithRemoteUser(
                                new Configuration(),
                                readonlyConfig.get(HdfsConfigOptions.REMOTE_USER),
                                (configuration, userGroupInformation) ->
                                        new HiveMetaStoreClient(hiveConf));
                log.info(
                        "Create HiveMetaStoreClient success with RemoteUser: {}",
                        readonlyConfig.get(HdfsConfigOptions.REMOTE_USER));
                return;
            }
            this.hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            String errorMsg =
                    String.format(
                            "Using this hive uris [%s] to initialize "
                                    + "hive metastore client instance failed",
                            metastoreUri);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED, errorMsg, e);
        } catch (MalformedURLException e) {
            String errorMsg =
                    String.format(
                            "Using this hive uris [%s], hive conf [%s] to initialize "
                                    + "hive metastore client instance failed",
                            metastoreUri,
                            readonlyConfig
                                    .getOptional(HiveSinkOptions.HIVE_SITE_PATH)
                                    .orElse(null));
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED, errorMsg, e);
        } catch (Exception e) {
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED,
                    "Login form kerberos failed",
                    e);
        }
    }

    public static HiveMetaStoreProxy getInstance(ReadonlyConfig config) {
        if (INSTANCE == null) {
            synchronized (HiveMetaStoreProxy.class) {
                if (INSTANCE == null) {
                    INSTANCE = new HiveMetaStoreProxy(config);
                }
            }
        }
        return INSTANCE;
    }

    public Table getTable(@NonNull String dbName, @NonNull String tableName) {
        try {
            return hiveMetaStoreClient.getTable(dbName, tableName);
        } catch (TException e) {
            String errorMsg =
                    String.format("Get table [%s.%s] information failed", dbName, tableName);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HIVE_TABLE_INFORMATION_FAILED, errorMsg, e);
        }
    }

    public void addPartitions(
            @NonNull String dbName, @NonNull String tableName, List<String> partitions)
            throws TException {
        for (String partition : partitions) {
            try {
                hiveMetaStoreClient.appendPartition(dbName, tableName, partition);
            } catch (AlreadyExistsException e) {
                log.warn("The partition {} are already exists", partition);
            }
        }
    }

    public void dropPartitions(
            @NonNull String dbName, @NonNull String tableName, List<String> partitions)
            throws TException {
        for (String partition : partitions) {
            hiveMetaStoreClient.dropPartition(dbName, tableName, partition, false);
        }
    }

    public synchronized void close() {
        if (Objects.nonNull(hiveMetaStoreClient)) {
            hiveMetaStoreClient.close();
            HiveMetaStoreProxy.INSTANCE = null;
        }
    }

    private boolean enableKerberos(ReadonlyConfig readonlyConfig) {
        boolean kerberosPrincipalEmpty =
                StringUtils.isBlank(readonlyConfig.get(HdfsConfigOptions.KERBEROS_PRINCIPAL));
        boolean kerberosKeytabPathEmpty =
                StringUtils.isBlank(readonlyConfig.get(HdfsConfigOptions.KERBEROS_KEYTAB_PATH));
        if (kerberosKeytabPathEmpty && kerberosPrincipalEmpty) {
            return false;
        }
        if (!kerberosPrincipalEmpty && !kerberosKeytabPathEmpty) {
            return true;
        }
        if (kerberosPrincipalEmpty) {
            throw new IllegalArgumentException("Please set kerberosPrincipal");
        }
        throw new IllegalArgumentException("Please set kerberosKeytabPath");
    }

    private boolean enableRemoteUser(ReadonlyConfig config) {
        return StringUtils.isNotBlank(config.get(HdfsConfigOptions.REMOTE_USER));
    }
}
