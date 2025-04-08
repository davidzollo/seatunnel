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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.starrocks;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

public class JdbcStarRocksTypeMapper implements JdbcDialectTypeMapper {

    private JdbcStarRocksTypeConverter typeConverter;

    public JdbcStarRocksTypeMapper() {
        this(JdbcStarRocksTypeConverter.INSTANCE);
    }

    public JdbcStarRocksTypeMapper(JdbcStarRocksTypeConverter typeConverter) {
        this.typeConverter = typeConverter;
    }

    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return typeConverter.convert(typeDefine);
    }

    @Override
    public Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnName = metadata.getColumnLabel(colIndex);
        // e.g. tinyint unsigned
        String nativeType = metadata.getColumnTypeName(colIndex);
        int isNullable = metadata.isNullable(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        if (Arrays.asList("CHAR", "VARCHAR", "ENUM").contains(nativeType)) {
            long octetLength = TypeDefineUtils.charTo4ByteLength((long) precision);
            precision = (int) Math.max(precision, octetLength);
        }

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(nativeType)
                        .dataType(nativeType)
                        .nullable(isNullable == ResultSetMetaData.columnNullable)
                        .length((long) precision)
                        .precision((long) precision)
                        .scale(scale)
                        .build();
        return mappingColumn(typeDefine);
    }

    protected Boolean getNullAble(Connection conn, TablePath tablePath, String columnName) {
        String sql =
                String.format(
                        "SHOW FULL COLUMNS FROM `%s`.`%s`",
                        tablePath.getDatabaseName(), tablePath.getTableName());
        try (final PreparedStatement preparedStatement = conn.prepareStatement(sql);
                final ResultSet rs = preparedStatement.executeQuery()) {
            while (rs.next()) {
                if (rs.getString("Field").equals(columnName)) {
                    return "Yes".equalsIgnoreCase(rs.getString("Null"));
                }
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing table in catalog %s", tablePath), e);
        }
        return true;
    }
}
