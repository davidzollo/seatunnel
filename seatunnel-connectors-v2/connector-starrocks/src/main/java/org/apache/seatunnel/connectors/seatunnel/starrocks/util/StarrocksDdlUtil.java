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

package org.apache.seatunnel.connectors.seatunnel.starrocks.util;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.StarRocksSaveModeUtil;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class StarrocksDdlUtil {

    public static void executeDdl(
            SinkConfig sinkConfig, SchemaChangeEvent event, CatalogTable catalogTable) {
        try (Connection conn =
                DriverManager.getConnection(
                        sinkConfig.getJdbcUrl(),
                        sinkConfig.getUsername(),
                        sinkConfig.getPassword())) {
            final List<String> ddlSqlList = getDdlSqlList(conn, event, catalogTable);
            if (!CollectionUtils.isEmpty(ddlSqlList)) {
                executeDdlSql(conn, ddlSqlList);
            }
        } catch (SQLException e) {
            if (e.getMessage().contains("Nothing is changed")) {
                log.warn(e.getMessage(), e);
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private static List<String> getDdlSqlList(
            Connection connection, SchemaChangeEvent event, CatalogTable catalogTable) {
        TablePath tablePath = catalogTable.getTableId().toTablePath();
        return getSQLFromSchemaChangeEvent(connection, tablePath, event);
    }

    private static void executeDdlSql(Connection connection, List<String> ddlSqlList)
            throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (String ddlSql : ddlSqlList) {
                statement.execute(ddlSql);
            }
        }
    }

    private static List<String> getSQLFromSchemaChangeEvent(
            Connection connection, TablePath tablePath, SchemaChangeEvent event) {
        List<String> sqlList = new ArrayList<>();
        if (event instanceof AlterTableColumnsEvent) {
            ((AlterTableColumnsEvent) event)
                    .getEvents()
                    .forEach(
                            column -> {
                                if (column instanceof AlterTableChangeColumnEvent) {
                                    AlterTableChangeColumnEvent changeColumnEvent =
                                            (AlterTableChangeColumnEvent) column;
                                    if (!changeColumnEvent
                                            .getOldColumn()
                                            .equals(changeColumnEvent.getColumn().getName())) {
                                        if (!columnExists(
                                                        connection,
                                                        tablePath,
                                                        changeColumnEvent.getOldColumn())
                                                && columnExists(
                                                        connection,
                                                        tablePath,
                                                        changeColumnEvent.getColumn().getName())) {
                                            log.warn(
                                                    "Column {} does not exist in table {}, Skip change column event",
                                                    changeColumnEvent.getOldColumn(),
                                                    tablePath.getFullName());
                                            return;
                                        }
                                    }
                                    String sql =
                                            String.format(
                                                    "alter table %s RENAME COLUMN %s %s",
                                                    tablePath.getFullName(),
                                                    changeColumnEvent.getOldColumn(),
                                                    changeColumnEvent.getColumn().getName());
                                    sqlList.add(sql);
                                } else if (column instanceof AlterTableModifyColumnEvent) {
                                    String sql =
                                            String.format(
                                                    "alter table %s MODIFY COLUMN %s",
                                                    tablePath.getFullName(),
                                                    StarRocksSaveModeUtil.columnToStarrocksType(
                                                            ((AlterTableAddColumnEvent) column)
                                                                    .getColumn()));
                                    sqlList.add(sql);
                                } else if (column instanceof AlterTableAddColumnEvent) {
                                    AlterTableAddColumnEvent addColumnEvent =
                                            (AlterTableAddColumnEvent) column;
                                    if (columnExists(
                                            connection,
                                            tablePath,
                                            addColumnEvent.getColumn().getName())) {
                                        log.warn(
                                                "Column {} already exists in table {}, Skip add column event",
                                                addColumnEvent.getColumn().getName(),
                                                tablePath.getFullName());
                                        return;
                                    }
                                    String sql =
                                            String.format(
                                                    "alter table %s add column %s DEFAULT %s",
                                                    tablePath.getFullName(),
                                                    StarRocksSaveModeUtil.columnToStarrocksType(
                                                            addColumnEvent.getColumn()),
                                                    getDefaultValue(
                                                            addColumnEvent
                                                                    .getColumn()
                                                                    .getDefaultValue()));
                                    sqlList.add(sql);
                                } else if (column instanceof AlterTableDropColumnEvent) {
                                    AlterTableDropColumnEvent dropColumnEvent =
                                            (AlterTableDropColumnEvent) column;
                                    if (!columnExists(
                                            connection, tablePath, dropColumnEvent.getColumn())) {
                                        log.warn(
                                                "Column {} does not exist in table {}, Skip drop column event",
                                                dropColumnEvent.getColumn(),
                                                tablePath.getFullName());
                                        return;
                                    }
                                    String sql =
                                            String.format(
                                                    "alter table %s drop column %s",
                                                    tablePath.getFullName(),
                                                    dropColumnEvent.getColumn());
                                    sqlList.add(sql);
                                } else {
                                    throw new UnsupportedOperationException(
                                            "Unsupported event: " + event);
                                }
                            });
        }
        return sqlList;
    }

    private static String getDefaultValue(Object defaultValue) {
        if (defaultValue == null) {
            return "null";
        }
        return String.format("\"%s\"", defaultValue.toString());
    }

    private static boolean columnExists(Connection connection, TablePath tablePath, String column) {
        String selectColumnSQL =
                String.format("SELECT %s FROM %s WHERE 1 != 1", column, tablePath.getFullName());
        try (Statement statement = connection.createStatement()) {
            return statement.execute(selectColumnSQL);
        } catch (SQLException e) {
            log.info("Column {} does not exist in table {}", column, tablePath.getFullName(), e);
            return false;
        }
    }
}
