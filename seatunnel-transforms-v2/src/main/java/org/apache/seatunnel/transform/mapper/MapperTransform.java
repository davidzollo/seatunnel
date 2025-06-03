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

package org.apache.seatunnel.transform.mapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.SeaTunnelRowContainerGenerator;
import org.apache.seatunnel.transform.exception.MapperError;
import org.apache.seatunnel.transform.sql.SQLTransform;
import org.apache.seatunnel.transform.sql.SQLTransformConfig;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.common.CommonOptions.SOURCE_TABLE_NAME;
import static org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil.convertSqlTypeToSeaTunnelDataType;

@Slf4j
public class MapperTransform extends MultipleFieldOutputTransform {
    public static final String PLUGIN_NAME = "Mapper";

    private SQLTransform sqlTransform;
    private final TableIdentifier inputTableIdentifier;
    private final TableIdentifier outputTableIdentifier;
    private final List<Column> inputColumns;
    private final List<ColumnWrapper> outputColumns;
    private final MapperConfig.SpecificModify specificModified;

    public MapperTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.inputTableIdentifier = inputCatalogTable.getTableId();
        this.inputColumns = inputCatalogTable.getTableSchema().getColumns();

        // Try to find specific modification; if none, leave as null (no-transform mode)
        this.specificModified =
                config.get(MapperConfig.SPECIFIC).stream()
                        .filter(
                                specificModify -> {
                                    TablePath tablePath =
                                            TablePath.of(specificModify.getInputName(), true);
                                    return tablePath
                                                    .getTableName()
                                                    .equals(inputTableIdentifier.getTableName())
                                            && (StringUtils.isBlank(tablePath.getSchemaName())
                                                    || tablePath
                                                            .getSchemaName()
                                                            .equals(
                                                                    inputTableIdentifier
                                                                            .getSchemaName()));
                                })
                        .findFirst()
                        .orElse(null);

        this.outputColumns = initColumns();
        // Initialize identifiers and columns based on whether modifications exist
        if (specificModified != null) {
            this.outputTableIdentifier = initTableIdentifier(inputCatalogTable);
        } else {
            // No modifications: passthrough
            this.outputTableIdentifier = inputTableIdentifier;
        }

        initSqlTransform(inputCatalogTable);
    }

    private TableIdentifier initTableIdentifier(CatalogTable inputCatalogTable) {
        String[] split = specificModified.getOutputName().split("\\.");
        if (split.length == 1) {
            return TableIdentifier.of(
                    inputCatalogTable.getCatalogName(),
                    inputCatalogTable.getTableId().getDatabaseName(),
                    inputCatalogTable.getTableId().getSchemaName(),
                    split[0]);
        } else if (split.length == 2) {
            return TableIdentifier.of(
                    inputCatalogTable.getCatalogName(),
                    inputCatalogTable.getTableId().getDatabaseName(),
                    split[0],
                    split[1]);
        } else {
            throw new IllegalArgumentException(
                    "Invalid output table identifier: " + specificModified.getOutputName());
        }
    }

    private List<ColumnWrapper> initColumns() {
        AtomicInteger position = new AtomicInteger();
        List<ColumnWrapper> collect =
                inputColumns.stream()
                        .map(
                                column -> {
                                    position.getAndIncrement();
                                    MapperConfig.Column def =
                                            MapperConfig.Column.builder()
                                                    .inputName(column.getName())
                                                    .outputName(column.getName())
                                                    .position(position.get())
                                                    .dataType(column.getDataType().getSqlType())
                                                    .length(column.getColumnLength())
                                                    .scale(column.getScale())
                                                    .nullable(column.isNullable())
                                                    .sinkType(column.getSinkType())
                                                    .defaultValue(column.getDefaultValue())
                                                    .comment(column.getComment())
                                                    .build();
                                    ColumnWrapper cw = new ColumnWrapper();
                                    cw.setColumn(def);
                                    cw.setDataType(column.getDataType());
                                    return cw;
                                })
                        .collect(Collectors.toList());

        if (specificModified != null) {
            for (MapperConfig.Column conditionColumn : specificModified.getColumns()) {
                switch (conditionColumn.getAction()) {
                    case ADD:
                        collect.add(
                                ColumnWrapper.of(
                                        MapperConfig.Column.builder()
                                                .inputName(conditionColumn.getInputName())
                                                .outputName(conditionColumn.getOutputName())
                                                .position(position.getAndIncrement())
                                                .dataType(conditionColumn.getDataType())
                                                .dateFormat(conditionColumn.getDateFormat())
                                                .length(conditionColumn.getLength())
                                                .scale(conditionColumn.getScale())
                                                .nullable(conditionColumn.isNullable())
                                                .defaultValue(conditionColumn.getDefaultValue())
                                                .comment(conditionColumn.getComment())
                                                .sinkType(conditionColumn.getSinkType())
                                                .sqlFunction(conditionColumn.getSqlFunction())
                                                .build()));
                        break;

                    case MODIFY:
                        collect.stream()
                                .filter(
                                        cw ->
                                                cw.getColumn()
                                                        .getInputName()
                                                        .equals(conditionColumn.getInputName()))
                                .forEach(
                                        cw -> {
                                            MapperConfig.Column tgt = cw.getColumn();

                                            if (isPresent(conditionColumn.getPosition())) {
                                                tgt.setPosition(conditionColumn.getPosition());
                                            }
                                            if (StringUtils.isNotBlank(
                                                    conditionColumn.getOutputName())) {
                                                tgt.setOutputName(conditionColumn.getOutputName());
                                            }
                                            if (isPresent(conditionColumn.getDataType())
                                                    && conditionColumn.getDataType()
                                                            != tgt.getDataType()) {
                                                cw.setTypeChanged(true);
                                                tgt.setDataType(conditionColumn.getDataType());
                                            }
                                            if (StringUtils.isNotBlank(
                                                    conditionColumn.getDateFormat())) {
                                                tgt.setDateFormat(conditionColumn.getDateFormat());
                                            }
                                            if (isPresent(conditionColumn.getLength())) {
                                                tgt.setLength(conditionColumn.getLength());
                                            }
                                            if (isPresent(conditionColumn.getScale())) {
                                                tgt.setScale(conditionColumn.getScale());
                                            }
                                            tgt.setNullable(conditionColumn.isNullable());
                                            if (conditionColumn.getDefaultValue() != null) {
                                                tgt.setDefaultValue(
                                                        conditionColumn.getDefaultValue());
                                            }
                                            if (StringUtils.isNotBlank(
                                                    conditionColumn.getComment())) {
                                                tgt.setComment(conditionColumn.getComment());
                                            }
                                            if (StringUtils.isNotBlank(
                                                    conditionColumn.getSinkType())) {
                                                tgt.setSinkType(conditionColumn.getSinkType());
                                            }
                                            if (StringUtils.isNotBlank(
                                                    conditionColumn.getSqlFunction())) {
                                                tgt.setSqlFunction(
                                                        conditionColumn.getSqlFunction());
                                            }
                                        });
                        break;

                    case DROP:
                        collect.removeIf(
                                cw ->
                                        cw.getColumn()
                                                .getInputName()
                                                .equals(conditionColumn.getInputName()));
                        break;

                    default:
                        throw new IllegalArgumentException(
                                "Unsupported action: " + conditionColumn.getAction());
                }
            }
        }

        List<String> columnNames =
                collect.stream()
                        .map(cw -> cw.getColumn().getOutputName())
                        .collect(Collectors.toList());
        List<String> duplicateColumnNames =
                columnNames.stream()
                        .filter(name -> columnNames.indexOf(name) != columnNames.lastIndexOf(name))
                        .distinct()
                        .collect(Collectors.toList());
        if (!duplicateColumnNames.isEmpty()) {
            throw new IllegalArgumentException(
                    "Duplicate column names found: " + String.join(", ", duplicateColumnNames));
        }

        collect.sort(Comparator.comparing(o -> o.getColumn().getPosition()));
        return collect;
    }

    private static boolean isPresent(Object o) {
        return o != null;
    }

    private void initSqlTransform(CatalogTable inputCatalogTable) {
        String querySql =
                createQuerySql(
                        outputColumns,
                        Optional.ofNullable(specificModified)
                                .orElse(new MapperConfig.SpecificModify())
                                .getColumns());
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("query", querySql);
        cfg.put("engine", "ZETA");
        SQLTransformConfig sqlTransformConfig = SQLTransformConfig.of(ReadonlyConfig.fromMap(cfg));
        Map<String, Object> tableCfg = new HashMap<>();
        tableCfg.put(SOURCE_TABLE_NAME.key(), inputTableIdentifier.getTableName());
        this.sqlTransform =
                new SQLTransform(
                        sqlTransformConfig, ReadonlyConfig.fromMap(tableCfg), inputCatalogTable);
        sqlTransform.tryOpen();
    }

    private String createQuerySql(
            List<ColumnWrapper> outputColumns, List<MapperConfig.Column> columns) {
        StringBuilder querySql = new StringBuilder("SELECT ");
        List<MapperConfig.Column> referenceColumns =
                Optional.ofNullable(columns).orElse(Collections.emptyList()).stream()
                        .filter(
                                c ->
                                        c.getAction() == MapperConfig.Action.ADD
                                                || c.getAction() == MapperConfig.Action.MODIFY)
                        .collect(Collectors.toList());

        String[] fields =
                outputColumns.stream()
                        .map(
                                cw -> {
                                    MapperConfig.Column col = cw.getColumn();
                                    String fname = col.getOutputName();
                                    for (MapperConfig.Column ref : referenceColumns) {
                                        if (fname.equals(ref.getOutputName())) {
                                            if (StringUtils.isNotBlank(col.getDateFormat())) {
                                                if (col.getDataType() == SqlType.STRING) {
                                                    fname =
                                                            String.format(
                                                                    "FORMATDATETIME(%s, '%s') AS %s",
                                                                    ref.getInputName(),
                                                                    col.getDateFormat(),
                                                                    ref.getOutputName());
                                                } else if (col.getDataType() == SqlType.TIMESTAMP) {
                                                    fname =
                                                            String.format(
                                                                    "PARSEDATETIME(FORMATDATETIME(%s, '%s'), '%s') AS %s",
                                                                    ref.getInputName(),
                                                                    col.getDateFormat(),
                                                                    col.getDateFormat(),
                                                                    ref.getOutputName());
                                                }
                                            } else if (StringUtils.isNotBlank(
                                                    ref.getSqlFunction())) {
                                                fname =
                                                        ref.getSqlFunction()
                                                                + " AS "
                                                                + ref.getOutputName();
                                            } else if (cw.isTypeChanged()) {
                                                fname =
                                                        String.format(
                                                                "CAST(%s AS %s) AS %s",
                                                                ref.getInputName(),
                                                                col.getDataType(),
                                                                ref.getOutputName());
                                            } else {
                                                fname =
                                                        ref.getInputName()
                                                                + " AS "
                                                                + ref.getOutputName();
                                            }
                                            break;
                                        }
                                    }
                                    return fname;
                                })
                        .toArray(String[]::new);

        querySql.append(String.join(", ", fields))
                .append(" FROM ")
                .append("`")
                .append(inputTableIdentifier.getTableName())
                .append("`");
        return querySql.toString();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return outputTableIdentifier;
    }

    @Override
    protected TableSchema transformTableSchema() {
        Column[] outputColumns = getOutputColumns();
        outputFieldNames =
                Arrays.stream(outputColumns)
                        .map(Column::getName)
                        .collect(Collectors.toList())
                        .toArray(TYPE_ARRAY_STRING);

        this.fieldsIndex = new int[outputColumns.length];
        for (int i = 0; i < outputColumns.length; i++) {
            this.fieldsIndex[i] = i;
        }

        TableSchema.Builder builder = TableSchema.builder();
        if (inputCatalogTable.getTableSchema().getPrimaryKey() != null) {
            builder.primaryKey(getOutputPrimaryKey());
        }

        List<ConstraintKey> copiedConstraintKeys = getOutputConstraintKey();
        builder.constraintKey(copiedConstraintKeys);

        TableSchema outputTableSchema =
                builder.columns(Arrays.stream(outputColumns).collect(Collectors.toList())).build();
        rowContainerGenerator =
                new SeaTunnelRowContainerGenerator() {
                    @Override
                    public SeaTunnelRow apply(SeaTunnelRow inputRow) {
                        Object[] outputFieldValues = new Object[outputColumns.length];
                        System.arraycopy(
                                inputRow.getFields(),
                                0,
                                outputFieldValues,
                                0,
                                outputColumns.length);

                        SeaTunnelRow outputRow = new SeaTunnelRow(outputFieldValues);
                        outputRow.setTableId(generateRowContainerTableId(inputRow.getTableId()));
                        outputRow.setRowKind(inputRow.getRowKind());
                        return outputRow;
                    }
                };

        return outputTableSchema;
    }

    private String generateRowContainerTableId(String tableId) {
        return outputTableIdentifier.toTablePath().toString();
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        if (specificModified != null) {
            return sqlTransform.transformRow(new SeaTunnelRow(inputRow.getFields())).getFields();
        } else {
            return inputRow.getFields();
        }
    }

    @Override
    protected Column[] getOutputColumns() {
        if (specificModified == null
                || specificModified.getColumns() == null
                || specificModified.getColumns().isEmpty()) {
            return inputColumns.toArray(new Column[0]);
        }
        return outputColumns.stream()
                .map(
                        cw -> {
                            MapperConfig.Column col = cw.getColumn();
                            SeaTunnelDataType<?> dt =
                                    cw.getDataType() != null
                                            ? cw.getDataType()
                                            : convertSqlTypeToSeaTunnelDataType(col.getDataType());

                            return PhysicalColumn.of(
                                    col.getOutputName(),
                                    dt,
                                    col.getLength(),
                                    col.getScale(),
                                    col.isNullable(),
                                    col.getDefaultValue(),
                                    col.getComment(),
                                    col.getSinkType());
                        })
                .toArray(Column[]::new);
    }

    @Override
    protected List<ConstraintKey> getOutputConstraintKey() {
        if (specificModified == null
                || specificModified.getIndexes() == null
                || specificModified.getIndexes().isEmpty()) {
            return super.getOutputConstraintKey();
        }
        List<ConstraintKey> added =
                specificModified.getIndexes().stream()
                        .filter(idx -> idx.getAction() == MapperConfig.Action.ADD)
                        .map(MapperConfig.Index::copy)
                        .map(
                                idx ->
                                        ConstraintKey.of(
                                                idx.isUnique()
                                                        ? ConstraintKey.ConstraintType.UNIQUE_KEY
                                                        : ConstraintKey.ConstraintType.INDEX_KEY,
                                                idx.getName(),
                                                idx.getColumns().stream()
                                                        .map(
                                                                MapperConfig.ReferenceColumn
                                                                        ::toConstraintKeyColumn)
                                                        .collect(Collectors.toList())))
                        .collect(Collectors.toList());
        List<String> dropped =
                specificModified.getIndexes().stream()
                        .filter(idx -> idx.getAction() == MapperConfig.Action.DROP)
                        .map(MapperConfig.Index::getName)
                        .collect(Collectors.toList());
        List<ConstraintKey> modified =
                specificModified.getIndexes().stream()
                        .filter(idx -> idx.getAction() == MapperConfig.Action.MODIFY)
                        .map(MapperConfig.Index::copy)
                        .map(
                                idx ->
                                        ConstraintKey.of(
                                                idx.isUnique()
                                                        ? ConstraintKey.ConstraintType.UNIQUE_KEY
                                                        : ConstraintKey.ConstraintType.INDEX_KEY,
                                                idx.getName(),
                                                idx.getColumns().stream()
                                                        .map(
                                                                MapperConfig.ReferenceColumn
                                                                        ::toConstraintKeyColumn)
                                                        .collect(Collectors.toList())))
                        .collect(Collectors.toList());
        return mergeConstraintKeys(
                inputCatalogTable.getTableSchema().getConstraintKeys(), added, dropped, modified);
    }

    @Override
    protected PrimaryKey getOutputPrimaryKey() {
        if (specificModified == null || specificModified.getPrimaryKey() == null) {
            return super.getOutputPrimaryKey();
        }
        switch (specificModified.getPrimaryKey().getAction()) {
            case ADD:
            case MODIFY:
                return PrimaryKey.of(
                        specificModified.getPrimaryKey().getName(),
                        specificModified.getPrimaryKey().getColumns().stream()
                                .map(MapperConfig.ReferenceColumn::getReferenceName)
                                .collect(Collectors.toList()));
            case DROP:
                return null;
            default:
                throw new IllegalArgumentException(
                        "Unsupported action: " + specificModified.getPrimaryKey().getAction());
        }
    }

    @Override
    protected String transformComment() {
        if (specificModified == null || specificModified.getComment() == null) {
            return inputCatalogTable.getComment();
        }
        switch (specificModified.getComment().getAction()) {
            case ADD:
            case MODIFY:
                return specificModified.getComment().getContent();
            case DROP:
                return null;
            default:
                throw new IllegalArgumentException(
                        "Unsupported action: " + specificModified.getComment().getAction());
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        SeaTunnelRowType deductionOutputRowType =
                sqlTransform.getOutputRowTypeAndColumns(new ArrayList<>());
        Map<String, List<Map<String, String>>> wrongField = new LinkedHashMap<>();
        for (int i = 0; i < outputColumns.size(); i++) {
            SeaTunnelDataType<?> fieldType = deductionOutputRowType.getFieldType(i);
            String fieldName = deductionOutputRowType.getFieldNames()[i];
            ColumnWrapper cw = outputColumns.get(i);
            boolean typeMatches = fieldType.getSqlType() == cw.column.getDataType();
            boolean nameMatches = Objects.equals(fieldName, cw.column.getOutputName());
            if (!typeMatches || !nameMatches) {
                Map<String, String> detail = new LinkedHashMap<>();
                detail.put("specify_type", cw.column.getDataType().toString());
                detail.put("actual_type", fieldType.getSqlType().toString());
                detail.put("field_name", cw.column.getInputName());
                detail.put("sql_function", cw.column.getSqlFunction());
                String tableName =
                        StringUtils.defaultIfBlank(
                                outputTableIdentifier.getTableName(),
                                inputTableIdentifier.getTableName());
                wrongField.computeIfAbsent(tableName, k -> new ArrayList<>()).add(detail);
            }
        }
        if (!wrongField.isEmpty()) {
            throw MapperError.fieldWithWrongSqlFunction(wrongField);
        }
        return super.getProducedCatalogTable();
    }

    @Data
    private static class ColumnWrapper implements Serializable {
        private MapperConfig.Column column;
        private SeaTunnelDataType<?> dataType;
        private boolean typeChanged = false;

        public static ColumnWrapper of(MapperConfig.Column column) {
            ColumnWrapper cw = new ColumnWrapper();
            cw.setColumn(column);
            return cw;
        }
    }

    private List<ConstraintKey> mergeConstraintKeys(
            List<ConstraintKey> existing,
            List<ConstraintKey> added,
            List<String> dropped,
            List<ConstraintKey> modified) {
        List<ConstraintKey> merged =
                existing.stream()
                        .filter(ck -> !dropped.contains(ck.getConstraintName()))
                        .collect(Collectors.toList());
        merged.addAll(added);
        for (ConstraintKey m : modified) {
            merged.removeIf(ck -> ck.getConstraintName().equals(m.getConstraintName()));
            merged.add(m);
        }
        return merged;
    }
}
