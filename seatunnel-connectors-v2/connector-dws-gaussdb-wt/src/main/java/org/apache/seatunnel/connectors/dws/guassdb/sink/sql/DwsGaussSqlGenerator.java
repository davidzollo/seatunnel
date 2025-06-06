/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.dws.guassdb.sink.sql;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBTypeConverter;
import org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

public class DwsGaussSqlGenerator implements Serializable {

    private final CatalogTable catalogTable;
    private final DwsGaussDBSinkOption.FieldIdeEnum fieldIdeEnum;
    // todo: use primary key in catalog table
    private final String primaryKey;
    private final String schemaName;
    private final String templateTableName;

    private final String targetTableName;

    private final String delimiter;

    private transient Map<Integer, Function<Object, String>> transformToCopyStringFunction;

    public DwsGaussSqlGenerator(
            String primaryKey,
            DwsGaussDBSinkOption.FieldIdeEnum fieldIdeEnum,
            CatalogTable catalogTable,
            String delimiter) {
        this.fieldIdeEnum = checkNotNull(fieldIdeEnum);
        if (StringUtils.isNotEmpty(primaryKey)) {
            this.primaryKey = getIDEString(primaryKey);
        } else {
            this.primaryKey = primaryKey;
        }
        this.delimiter = delimiter;
        this.catalogTable = catalogTable;
        this.schemaName =
                Optional.ofNullable(catalogTable.getTableId().getSchemaName())
                        .map(this::getIDEString)
                        .orElse("default");
        this.targetTableName = getIDEString(catalogTable.getTableId().getTableName());
        this.templateTableName = getIDEString("st_temporary_" + targetTableName);
        this.transformToCopyStringFunction = initializeTransformToCopyStringFunction();
    }

    public String getTemporaryTableName() {
        return templateTableName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public String getCopyInTemporaryTableSql() {
        return "COPY \""
                + schemaName
                + "\".\""
                + templateTableName
                + "\""
                + "("
                + Stream.concat(
                                catalogTable.getTableSchema().getColumns().stream()
                                        .map(c -> c.getName()),
                                Stream.of("st_snapshot_id", "st_is_deleted"))
                        .map(column -> getIDEString(column))
                        .collect(Collectors.joining(","))
                + ")"
                + " FROM STDIN"
                + " WITH(format 'text', delimiter E'"
                + delimiter
                + "', noescaping 'true', compatible_illegal_chars 'true')";
    }

    public String getCopyInTargetTableSql() {
        return "COPY \""
                + schemaName
                + "\".\""
                + targetTableName
                + "\""
                + "("
                + catalogTable.getTableSchema().getColumns().stream()
                        .map(column -> getIDEString(column.getName()))
                        .collect(Collectors.joining(","))
                + ")"
                + " FROM STDIN"
                + " WITH(format 'text', delimiter E'"
                + delimiter
                + "', noescaping 'true', compatible_illegal_chars 'true')";
    }

    public String getMergeInTargetTableSql(Long snapshotId) {
        String sql =
                "INSERT INTO %s SELECT %s FROM %s WHERE st_snapshot_id = %s "
                        + "ON CONFLICT(%s) "
                        + "DO UPDATE SET %s;";

        // inject table
        String targetTable = "\"" + schemaName + "\".\"" + targetTableName + "\"";
        String temporaryTable = "\"" + schemaName + "\".\"" + templateTableName + "\"";

        List<String> updateColumns = new ArrayList<>();
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        List<String> columnNames =
                columns.stream()
                        .map(column -> getIDEString(column.getName()))
                        .collect(Collectors.toList());
        for (String columnName : columnNames) {
            if (columnName.equals(primaryKey)) {
                // the primary key doesn't need to update
                continue;
            }
            updateColumns.add(columnName + "=" + "EXCLUDED." + columnName);
        }

        return String.format(
                sql,
                targetTable,
                columnNames.stream().collect(Collectors.joining(",")),
                temporaryTable,
                snapshotId,
                primaryKey,
                String.join(",", updateColumns));
    }

    public String getTemporaryRows(
            Collection<SeaTunnelRow> seaTunnelRows, boolean isDeleteRow, Long snapshotId) {
        return seaTunnelRows.stream()
                .map(
                        seaTunnelRow ->
                                appendRowInTemporaryTable(seaTunnelRow, isDeleteRow, snapshotId))
                .collect(Collectors.joining("\n"));
    }

    public String getTargetTableRows(Collection<SeaTunnelRow> seaTunnelRows) {
        return seaTunnelRows.stream()
                .map(this::appendRowInTargetTable)
                .collect(Collectors.joining("\n"));
    }

    public String getDeleteTemporarySnapshotSql(List<Long> snapshotId) {
        return "DELETE FROM \""
                + schemaName
                + "\".\""
                + templateTableName
                + "\" WHERE st_snapshot_id in ("
                + snapshotId.stream().map(String::valueOf).collect(Collectors.joining(","))
                + ")";
    }

    public String getDeleteTargetTableSql() {
        return "DELETE FROM \"" + schemaName + "\".\"" + targetTableName + "\"";
    }

    public String getDeleteTemporaryTableSql() {
        return "DELETE FROM \"" + schemaName + "\".\"" + templateTableName + "\"";
    }

    public String getDropTemporaryTableSql() {
        return "DROP TABLE IF EXISTS \"" + schemaName + "\".\"" + templateTableName + "\"";
    }

    public String getDropTargetTableSql() {
        return "DROP TABLE IF EXISTS \"" + schemaName + "\".\"" + targetTableName + "\"";
    }

    public String getQuertTargetTableDataCountSql() {
        return "SELECT COUNT(*) FROM \"" + schemaName + "\".\"" + targetTableName + "\"";
    }

    public String getDeleteRowsInTargetTableSql(Long currentSnapshotId) {
        // todo: only support one primary key
        return "DELETE FROM \""
                + schemaName
                + "\".\""
                + targetTableName
                + "\" WHERE "
                + primaryKey
                + " IN (SELECT "
                + primaryKey
                + " FROM \""
                + schemaName
                + "\".\""
                + templateTableName
                + "\" WHERE st_snapshot_id = "
                + currentSnapshotId
                + " AND st_is_deleted = true)";
    }

    public String getDeleteRowsInTemporaryTableSql(Long currentSnapshotId) {
        return "DELETE FROM \""
                + schemaName
                + "\".\""
                + templateTableName
                + "\" WHERE st_snapshot_id = "
                + currentSnapshotId
                + " AND st_is_deleted = true";
    }

    public String getCreateTemporaryTableSql() {
        StringBuilder createTemporaryTableSql = new StringBuilder();

        createTemporaryTableSql
                .append("CREATE TABLE IF NOT EXISTS ")
                .append("\"" + schemaName + "\".\"" + templateTableName + "\"")
                .append(" (\n");

        List<String> columnSqls =
                catalogTable.getTableSchema().getColumns().stream()
                        .map(this::buildColumnSql)
                        .collect(Collectors.toList());
        // add snapshot_id and is_deleted column
        columnSqls.add("\"" + getIDEString("st_snapshot_id") + "\" bigint");
        columnSqls.add("\"" + getIDEString("st_is_deleted") + "\" boolean");
        createTemporaryTableSql.append(String.join(",\n", columnSqls));
        createTemporaryTableSql.append("\n);");
        // add index for snapshot_id
        columnSqls.add("INDEX (" + getIDEString("st_snapshot_id") + ")");

        return createTemporaryTableSql.toString();
    }

    public String getCreateTargetTableSql() {

        StringBuilder createTemporaryTableSql = new StringBuilder();

        createTemporaryTableSql
                .append("CREATE TABLE IF NOT EXISTS ")
                .append(
                        getIDEString(
                                catalogTable
                                        .getTableId()
                                        .toTablePath()
                                        .getSchemaAndTableName("\"")))
                .append(" (\n");

        List<String> columnSqls =
                catalogTable.getTableSchema().getColumns().stream()
                        .map(this::buildColumnSql)
                        .collect(Collectors.toList());
        createTemporaryTableSql.append(String.join(",\n", columnSqls));
        createTemporaryTableSql.append("\n);");

        return createTemporaryTableSql.toString();
    }

    private String appendRowInTemporaryTable(
            SeaTunnelRow seaTunnelRow, boolean isDeleted, Long snapshotId) {
        if (transformToCopyStringFunction == null) {
            synchronized (this) {
                transformToCopyStringFunction = initializeTransformToCopyStringFunction();
            }
        }

        StringBuilder stringBuilder = new StringBuilder();
        Object[] fields = seaTunnelRow.getFields();
        for (int i = 0; i < fields.length; i++) {
            Object field = seaTunnelRow.getField(i);
            String fieldStr = transformToCopyStringFunction.get(i).apply(field);
            if (fieldStr == null) {
                // use '' represent null
            } else {
                stringBuilder.append(fieldStr);
            }
            stringBuilder.append(delimiter);
        }
        // todo: If the schema changed, we need to make sure the snapshotId and isDeleted flag is
        // the last two column
        stringBuilder.append(snapshotId);
        stringBuilder.append(delimiter);
        stringBuilder.append(isDeleted);
        return stringBuilder.toString().replace("\n", "");
    }

    private String appendRowInTargetTable(SeaTunnelRow seaTunnelRow) {
        if (transformToCopyStringFunction == null) {
            synchronized (this) {
                transformToCopyStringFunction = initializeTransformToCopyStringFunction();
            }
        }
        Object[] fields = seaTunnelRow.getFields();
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            final Object field = seaTunnelRow.getField(i);
            String fieldStr = transformToCopyStringFunction.get(i).apply(field);
            // use '' represent null
            if (fieldStr != null) {
                stringBuilder.append(fieldStr);
            }
            if (i != fields.length - 1) {
                stringBuilder.append(delimiter);
            }
        }
        return stringBuilder.toString().replace("\n", "");
    }

    private String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql
                .append("\"")
                .append(getIDEString(column.getName()))
                .append("\" ")
                .append(buildColumnType(column));

        // Add NOT NULL if column is not nullable
        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        // Add primary key directly after the column if it is a primary key
        if (StringUtils.isNotEmpty(primaryKey)) {
            if (primaryKey.equals(getIDEString(column.getName()))) {
                columnSql.append(" PRIMARY KEY");
            }
        }

        return columnSql.toString();
    }

    String buildColumnType(Column column) {
        String columnType;
        if (column.getSinkType() != null) {
            columnType = column.getSinkType();
        } else {
            columnType = DwsGaussDBTypeConverter.INSTANCE.reconvert(column).getColumnType();
        }
        return columnType;
    }

    private String getIDEString(String originString) {
        if (originString == null) {
            return originString;
        }
        switch (fieldIdeEnum) {
            case ORIGINAL:
                return originString;
            case LOWERCASE:
                return originString.toLowerCase();
            case UPPERCASE:
                return originString.toUpperCase();
            default:
                return originString;
        }
    }

    private Map<Integer, Function<Object, String>> initializeTransformToCopyStringFunction() {
        Map<Integer, Function<Object, String>> map = new HashMap<>();
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Function<Object, String> function = null;
            Column column = columns.get(i);
            if (column.getDataType().getSqlType() == SqlType.BYTES) {
                function =
                        new Function<Object, String>() {
                            @Override
                            public String apply(Object input) {
                                if (input == null) {
                                    return null;
                                }
                                return Base64.getEncoder().encodeToString((byte[]) input);
                            }
                        };
            } else if (column.getDataType().getSqlType() != SqlType.ARRAY) {
                function =
                        new Function<Object, String>() {
                            @Override
                            public String apply(Object input) {
                                if (input == null) {
                                    return null;
                                }
                                return input.toString();
                            }
                        };
            } else {
                ArrayType dataType = (ArrayType) column.getDataType();
                SqlType elemantSqlType = dataType.getElementType().getSqlType();
                switch (elemantSqlType) {
                    case STRING:
                        function =
                                new Function<Object, String>() {
                                    @Override
                                    public String apply(Object input) {
                                        if (input == null) {
                                            return null;
                                        }
                                        String[] arr = (String[]) input;
                                        return Arrays.stream(arr)
                                                .collect(Collectors.joining(",", "{", "}"));
                                    }
                                };
                        break;
                    case INT:
                        function =
                                new Function<Object, String>() {
                                    @Override
                                    public String apply(Object input) {
                                        if (input == null) {
                                            return null;
                                        }
                                        Integer[] arr = (Integer[]) input;
                                        return Arrays.stream(arr)
                                                .map(Object::toString)
                                                .collect(Collectors.joining(",", "{", "}"));
                                    }
                                };
                        break;
                    case SMALLINT:
                        function =
                                new Function<Object, String>() {
                                    @Override
                                    public String apply(Object input) {
                                        if (input == null) {
                                            return null;
                                        }
                                        Short[] arr = (Short[]) input;
                                        return Arrays.stream(arr)
                                                .map(Object::toString)
                                                .collect(Collectors.joining(",", "{", "}"));
                                    }
                                };
                        break;
                    case BIGINT:
                        function =
                                new Function<Object, String>() {
                                    @Override
                                    public String apply(Object input) {
                                        if (input == null) {
                                            return null;
                                        }
                                        Long[] arr = (Long[]) input;
                                        return Arrays.stream(arr)
                                                .map(Object::toString)
                                                .collect(Collectors.joining(",", "{", "}"));
                                    }
                                };
                        break;
                    case FLOAT:
                        function =
                                new Function<Object, String>() {
                                    @Override
                                    public String apply(Object input) {
                                        if (input == null) {
                                            return null;
                                        }
                                        Float[] arr = (Float[]) input;
                                        return Arrays.stream(arr)
                                                .map(Object::toString)
                                                .collect(Collectors.joining(",", "{", "}"));
                                    }
                                };
                        break;
                    case BYTES:
                        function =
                                new Function<Object, String>() {
                                    @Override
                                    public String apply(Object input) {
                                        if (input == null) {
                                            return null;
                                        }
                                        Byte[] arr = (Byte[]) input;
                                        return Arrays.stream(arr)
                                                .map(Object::toString)
                                                .collect(Collectors.joining(",", "{", "}"));
                                    }
                                };
                        break;
                    case DOUBLE:
                        function =
                                new Function<Object, String>() {
                                    @Override
                                    public String apply(Object input) {
                                        if (input == null) {
                                            return null;
                                        }
                                        Double[] arr = (Double[]) input;
                                        return Arrays.stream(arr)
                                                .map(Object::toString)
                                                .collect(Collectors.joining(",", "{", "}"));
                                    }
                                };
                        break;
                    case BOOLEAN:
                        function =
                                new Function<Object, String>() {
                                    @Override
                                    public String apply(Object input) {
                                        if (input == null) {
                                            return null;
                                        }
                                        Boolean[] arr = (Boolean[]) input;
                                        return Arrays.stream(arr)
                                                .map(Object::toString)
                                                .collect(Collectors.joining(",", "{", "}"));
                                    }
                                };
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported array type: " + dataType);
                }
            }
            map.put(i, function);
        }
        return map;
    }
}
