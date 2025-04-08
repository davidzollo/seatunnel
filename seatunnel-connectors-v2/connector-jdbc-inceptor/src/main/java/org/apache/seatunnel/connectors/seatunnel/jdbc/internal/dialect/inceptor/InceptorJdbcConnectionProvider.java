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
package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.inceptor;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;

import io.transwarp.hadoop.conf.Configuration;
import io.transwarp.hadoop.security.UserGroupInformation;
import lombok.NonNull;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode.KERBEROS_AUTHENTICATION_FAILED;

public class InceptorJdbcConnectionProvider extends SimpleJdbcConnectionProvider {

    public InceptorJdbcConnectionProvider(@NonNull JdbcConnectionConfig jdbcConfig) {
        super(jdbcConfig);
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (jdbcConfig.isUseKerberos()) {
            InceptorJdbcUtils.doKerberosAuthentication(jdbcConfig);
        }
        if (isConnectionValid()) {
            return super.getConnection();
        }
        JdbcConnectionConfig jdbcConfig = super.getJdbcConfig();
        final Driver driver = getLoadedDriver();
        InceptorConnectionProduceFunction inceptorConnectionProduceFunction =
                new InceptorConnectionProduceFunction(driver, jdbcConfig);

        if (jdbcConfig.isUseKerberos()) {
            super.setConnection(getConnectionWithKerberos(inceptorConnectionProduceFunction));
        } else {
            super.setConnection(inceptorConnectionProduceFunction.produce());
        }
        if (super.getConnection() == null) {
            // Throw same exception as DriverManager.getConnection when no driver found to match
            // caller expectation.
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.NO_SUITABLE_DRIVER,
                    "No suitable driver found for " + super.getJdbcConfig().getUrl());
        }
        return super.getConnection();
    }

    private Connection getConnectionWithKerberos(
            InceptorConnectionProduceFunction hiveConnectionProduceFunction) {
        try {
            Configuration configuration = new Configuration();
            configuration.set("hadoop.security.authentication", "kerberos");
            return loginWithKerberos(
                    configuration,
                    jdbcConfig.getKrb5Path(),
                    jdbcConfig.getKerberosPrincipal(),
                    jdbcConfig.getKerberosKeytabPath(),
                    (conf, userGroupInformation) -> hiveConnectionProduceFunction.produce());
        } catch (Exception ex) {
            throw new JdbcConnectorException(KERBEROS_AUTHENTICATION_FAILED, ex);
        }
    }

    public static <T> T loginWithKerberos(
            Configuration configuration,
            String krb5FilePath,
            String kerberosPrincipal,
            String kerberosKeytabPath,
            LoginFunction<T> action)
            throws IOException, InterruptedException {
        if (!configuration.get("hadoop.security.authentication").equals("kerberos")) {
            throw new IllegalArgumentException("hadoop.security.authentication must be kerberos");
        }
        // Use global lock to avoid multiple threads to execute setConfiguration at the same time
        synchronized (UserGroupInformation.class) {
            System.setProperty("java.security.krb5.conf", krb5FilePath);
            // init configuration
            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation userGroupInformation =
                    UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                            kerberosPrincipal, kerberosKeytabPath);
            return userGroupInformation.doAs(
                    (PrivilegedExceptionAction<T>)
                            () -> action.run(configuration, userGroupInformation));
        }
    }

    public static class InceptorConnectionProduceFunction {

        private final Driver driver;
        private final JdbcConnectionConfig jdbcConnectionConfig;

        public InceptorConnectionProduceFunction(
                Driver driver, JdbcConnectionConfig jdbcConnectionConfig) {
            this.driver = driver;
            this.jdbcConnectionConfig = jdbcConnectionConfig;
        }

        public Connection produce() throws SQLException {
            final Properties info = new Properties();
            jdbcConnectionConfig
                    .getUsername()
                    .ifPresent(username -> info.setProperty("user", username));
            jdbcConnectionConfig
                    .getPassword()
                    .ifPresent(username -> info.setProperty("password", username));
            return driver.connect(jdbcConnectionConfig.getUrl(), info);
        }
    }

    public interface LoginFunction<T> {

        T run(Configuration configuration, UserGroupInformation userGroupInformation)
                throws Exception;
    }
}
