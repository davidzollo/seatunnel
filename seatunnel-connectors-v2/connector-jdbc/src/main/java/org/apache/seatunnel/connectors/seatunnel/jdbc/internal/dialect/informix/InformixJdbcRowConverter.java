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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.informix;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Optional;

public class InformixJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return DatabaseIdentifier.INFORMIX;
    }

    @Override
    @SuppressWarnings("checkstyle:Indentation")
    public SeaTunnelRow toInternal(ResultSet rs, TableSchema tableSchema) throws SQLException {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[typeInfo.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            int resultSetIndex = fieldIndex + 1;
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    fields[fieldIndex] = rs.getString(resultSetIndex);
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = rs.getBoolean(resultSetIndex);
                    break;
                case TINYINT:
                    fields[fieldIndex] = rs.getByte(resultSetIndex);
                    break;
                case SMALLINT:
                    fields[fieldIndex] = rs.getShort(resultSetIndex);
                    break;
                case INT:
                    fields[fieldIndex] = rs.getInt(resultSetIndex);
                    break;
                case BIGINT:
                    fields[fieldIndex] = rs.getLong(resultSetIndex);
                    break;
                case FLOAT:
                    fields[fieldIndex] = rs.getFloat(resultSetIndex);
                    break;
                case DOUBLE:
                    fields[fieldIndex] = rs.getDouble(resultSetIndex);
                    break;
                case DECIMAL:
                    fields[fieldIndex] = rs.getBigDecimal(resultSetIndex);
                    break;
                case DATE:
                    Date sqlDate = rs.getDate(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlDate).map(Date::toLocalDate).orElse(null);
                    break;
                case TIME:
                    Time sqlTime = rs.getTime(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTime).map(Time::toLocalTime).orElse(null);
                    break;
                case TIMESTAMP:
                    Timestamp sqlTimestamp = rs.getTimestamp(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTimestamp)
                                    .map(Timestamp::toLocalDateTime)
                                    .orElse(null);
                    break;
                case BYTES:
                    fields[fieldIndex] = rs.getBytes(resultSetIndex);
                    break;
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case MAP:
                case ARRAY:
                case ROW:
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType);
            }
        }
        return new SeaTunnelRow(fields);
    }

    /**
     * Override setValueToStatementByDataType to handle Informix-specific data types for Sink
     * functionality. This method provides enhanced support for Informix types like TEXT, BYTE,
     * BLOB, MONEY, etc.
     *
     * @param value The value to set
     * @param statement The PreparedStatement to set the value into
     * @param seaTunnelDataType The SeaTunnel data type
     * @param statementIndex The parameter index in the statement (1-based)
     * @param sourceType The source database type for special handling
     * @throws SQLException if database access error occurs
     */
    @Override
    protected void setValueToStatementByDataType(
            Object value,
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {

        if (value == null) {
            statement.setObject(statementIndex, null);
            return;
        }

        switch (seaTunnelDataType.getSqlType()) {
            case BYTES:
                handleBytesType(value, statement, statementIndex, sourceType);
                break;
            case STRING:
                handleStringType(value, statement, statementIndex, sourceType);
                break;
            case DECIMAL:
                handleDecimalType(value, statement, statementIndex, sourceType);
                break;
            case TIMESTAMP:
                handleTimestampType(value, statement, statementIndex, sourceType);
                break;
            default:
                // Use the parent class implementation for standard types
                super.setValueToStatementByDataType(
                        value, statement, seaTunnelDataType, statementIndex, sourceType);
                break;
        }
    }

    /** Handle Informix BYTE and BLOB types with proper stream handling. */
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
        } else {
            // For BYTE types, use setBytes
            statement.setBytes(statementIndex, bytes);
        }
    }

    /** Handle Informix string types including TEXT and CLOB with large text support. */
    private void handleStringType(
            Object value,
            PreparedStatement statement,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        String stringValue = value.toString();

        if ("TEXT".equalsIgnoreCase(sourceType) || "CLOB".equalsIgnoreCase(sourceType)) {
            // For large text types, use setCharacterStream if the value is very large
            // Informix has a threshold around 32KB for efficient handling
            if (stringValue.length() > 32767) {
                statement.setCharacterStream(
                        statementIndex, new StringReader(stringValue), stringValue.length());
            } else {
                statement.setString(statementIndex, stringValue);
            }
        } else {
            // For standard string types (CHAR, VARCHAR, NCHAR, NVARCHAR, LVARCHAR)
            statement.setString(statementIndex, stringValue);
        }
    }

    /** Handle Informix decimal types including MONEY with proper precision. */
    private void handleDecimalType(
            Object value,
            PreparedStatement statement,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        // MONEY type in Informix is handled as BigDecimal with special formatting
        // All decimal types (DECIMAL, NUMERIC, MONEY) use setBigDecimal
        statement.setBigDecimal(statementIndex, (java.math.BigDecimal) value);
    }

    /** Handle Informix DATETIME type with proper precision handling. */
    private void handleTimestampType(
            Object value,
            PreparedStatement statement,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        if ("DATETIME".equalsIgnoreCase(sourceType)) {
            // Informix DATETIME handling with proper precision
            if (value instanceof LocalDateTime) {
                statement.setTimestamp(statementIndex, Timestamp.valueOf((LocalDateTime) value));
            } else {
                statement.setTimestamp(statementIndex, (Timestamp) value);
            }
        } else {
            // Use parent implementation for standard timestamp handling
            super.setValueToStatementByDataType(
                    value,
                    statement,
                    org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_DATE_TIME_TYPE,
                    statementIndex,
                    sourceType);
        }
    }
}
