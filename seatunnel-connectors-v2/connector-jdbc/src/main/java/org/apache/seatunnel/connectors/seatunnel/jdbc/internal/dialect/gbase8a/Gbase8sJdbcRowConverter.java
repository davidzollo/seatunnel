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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.gbase8a;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.informix.InformixJdbcRowConverter;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Gbase8sJdbcRowConverter extends InformixJdbcRowConverter {
    @Override
    public String converterName() {
        return DatabaseIdentifier.GBASE_8S;
    }

    @Override
    protected void handleBytesType(
            Object value,
            PreparedStatement statement,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        byte[] bytes = (byte[]) value;

        if ("BLOB".equalsIgnoreCase(sourceType)) {
            // For BLOB types, use setBinaryStream for better performance with large data
            statement.setBinaryStream(
                    statementIndex, new ByteArrayInputStream(bytes), bytes.length);
        } else if ("CLOB".equalsIgnoreCase(sourceType)) {
            statement.setAsciiStream(statementIndex, new ByteArrayInputStream(bytes), bytes.length);
        } else {
            // For BYTE types, use setBytes
            statement.setBytes(statementIndex, bytes);
        }
    }
}
