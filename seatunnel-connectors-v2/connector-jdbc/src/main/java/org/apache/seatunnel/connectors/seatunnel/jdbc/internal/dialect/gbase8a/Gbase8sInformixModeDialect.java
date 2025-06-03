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

import org.apache.seatunnel.api.table.catalog.TablePath;

import org.apache.commons.lang3.StringUtils;

public class Gbase8sInformixModeDialect extends Gbase8sDialect {

    @Override
    public TablePath parse(String tablePath) {
        String[] paths = tablePath.split(":");
        if (paths.length == 2) {
            String[] schemaAndTable = paths[1].split("\\.");
            String schema = schemaAndTable.length > 0 ? schemaAndTable[0] : null;
            String table = schemaAndTable[schemaAndTable.length - 1];
            return TablePath.of(paths[0], schema, table);
        }
        TablePath path = TablePath.of(tablePath, false);
        return TablePath.of(path.getDatabaseName(), path.getTableName());
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        if (StringUtils.isBlank(tablePath.getDatabaseName())) {
            return tablePath.getTableName();
        }
        return String.format("%s:%s", tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    public String tableIdentifier(String database, String tableName) {
        return quoteIdentifier(tableName);
    }
}
