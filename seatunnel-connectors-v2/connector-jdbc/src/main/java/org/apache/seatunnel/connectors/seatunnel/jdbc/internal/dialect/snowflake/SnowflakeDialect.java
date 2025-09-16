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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.snowflake;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.util.Optional;

public class SnowflakeDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return DatabaseIdentifier.SNOWFLAKE;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new SnowflakeJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new SnowflakeTypeMapper();
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database,
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields,
            boolean isPrimaryKeyUpdated) {
        if (uniqueKeyFields == null || uniqueKeyFields.length == 0) {
            return Optional.empty();
        }

        String fullTableName = database != null ? database + "." + tableName : tableName;

        // Build the MERGE statement for Snowflake
        StringBuilder merge = new StringBuilder();
        merge.append("MERGE INTO ").append(fullTableName).append(" AS target ");
        merge.append("USING (SELECT ");

        // Add parameter placeholders for all fields
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) merge.append(", ");
            merge.append("? AS ").append(fieldNames[i]);
        }
        merge.append(") AS source ");

        // Add ON condition using unique key fields
        merge.append("ON ");
        for (int i = 0; i < uniqueKeyFields.length; i++) {
            if (i > 0) merge.append(" AND ");
            merge.append("target.")
                    .append(uniqueKeyFields[i])
                    .append(" = source.")
                    .append(uniqueKeyFields[i]);
        }

        // Add WHEN MATCHED UPDATE clause
        merge.append(" WHEN MATCHED THEN UPDATE SET ");
        boolean first = true;
        for (String field : fieldNames) {
            // Skip unique key fields in UPDATE clause unless primary key updates are allowed
            boolean isUniqueKey = false;
            for (String uniqueKey : uniqueKeyFields) {
                if (uniqueKey.equals(field)) {
                    isUniqueKey = true;
                    break;
                }
            }
            if (!isUniqueKey || isPrimaryKeyUpdated) {
                if (!first) merge.append(", ");
                merge.append(field).append(" = source.").append(field);
                first = false;
            }
        }

        // Add WHEN NOT MATCHED INSERT clause
        merge.append(" WHEN NOT MATCHED THEN INSERT (");
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) merge.append(", ");
            merge.append(fieldNames[i]);
        }
        merge.append(") VALUES (");
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) merge.append(", ");
            merge.append("source.").append(fieldNames[i]);
        }
        merge.append(")");

        return Optional.of(merge.toString());
    }
}
