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
package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mrshive;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.hive.HiveJdbcUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.jdbc.HiveDriver;

import lombok.NonNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class MrsHiveJdbcConnectionProvider extends SimpleJdbcConnectionProvider {

    public MrsHiveJdbcConnectionProvider(@NonNull JdbcConnectionConfig jdbcConfig) {
        super(jdbcConfig);
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (jdbcConfig.useKerberos) {
            HiveJdbcUtils.doKerberosAuthentication(jdbcConfig);
        }
        Class.forName("org.apache.seatunnel.shade.mrs.org.apache.hive.jdbc.HiveDriver");
        if (isConnectionValid()) {
            return super.getConnection();
        }
        super.setConnection(getConnection());
        return super.getConnection();
    }

    public Connection getConnection() {
        String jdbcUrl = getJdbcUrl();
        try {
            if (useKerberos()) {
                String kerberosKrb5ConfPath = jdbcConfig.krb5Path;
                System.setProperty("java.security.krb5.conf", kerberosKrb5ConfPath);
            }
            HiveDriver driver = new HiveDriver();
            return driver.connect(jdbcUrl, new Properties());
        } catch (Exception e) {
            throw new SeaTunnelException("Get connection  failed: " + jdbcUrl, e);
        }
    }

    private String getJdbcUrl() {
        String url = jdbcConfig.getUrl();
        if (!useKerberos()) {
            return url;
        }
        String principal = jdbcConfig.kerberosPrincipal;
        StringBuilder stringBuilder = new StringBuilder(url);
        String username = jdbcConfig.getUsername().get();
        String kerberosKeytabPath = jdbcConfig.kerberosKeytabPath;
        stringBuilder
                .append(";principal=")
                .append(principal)
                .append(";user.principal=")
                .append(username)
                .append(";user.keytab=")
                .append(kerberosKeytabPath)
                .append(";");

        return stringBuilder.toString();
    }

    private boolean useKerberos() {
        return StringUtils.isNotEmpty(jdbcConfig.kerberosPrincipal);
    }
}
