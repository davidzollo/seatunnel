package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.inceptor;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.inceptor.InceptorTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.inceptor.StoredType;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class InceptorCreateTableSqlBuilder {
    private static final StoredType DEFAULT_STORED_TYPE = StoredType.ORC;

    private String tableName;
    private List<Column> columns;
    private String comment;
    private String sourceCatalogName;
    private String fieldIde;
    private List<String> partitionKeys;

    public InceptorCreateTableSqlBuilder(String tableName, CatalogTable catalogTable) {
        checkNotNull(tableName, "tableName must not be null");
        this.tableName = tableName;
        this.columns = catalogTable.getTableSchema().getColumns();
        this.comment = catalogTable.getComment();
        this.sourceCatalogName = catalogTable.getCatalogName();
        this.fieldIde = catalogTable.getOptions().get("fieldIde");
        this.partitionKeys = catalogTable.getPartitionKeys();
    }

    public String build() {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql
                .append("CREATE TABLE IF NOT EXISTS ")
                .append(CatalogUtils.quoteIdentifier(tableName, fieldIde, "`"))
                .append(" (\n");
        List<String> columnSqls =
                columns.stream()
                        .map(
                                column ->
                                        CatalogUtils.quoteIdentifier(
                                                buildColumnSql(column), fieldIde))
                        .collect(Collectors.toList());

        createTableSql.append(String.join(",\n", columnSqls));
        createTableSql.append("\n)").append("\n");
        if (comment != null) {
            createTableSql.append("COMMENT '").append(comment).append("'").append("\n");
        }
        if (partitionKeys != null && !partitionKeys.isEmpty()) {
            createTableSql.append("PARTITIONED BY (");
            StringJoiner partitions = new StringJoiner(",");
            partitionKeys.forEach(
                    partition -> {
                        // TODO partition type?
                        partitions.add(partition + " string");
                    });
            createTableSql.append(partitions.toString());
            createTableSql.append(")").append("\n");
        }
        createTableSql.append("STORED AS ").append(DEFAULT_STORED_TYPE.name()).append("\n");
        return createTableSql.toString();
    }

    private String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql.append("`").append(column.getName()).append("` ");

        // For simplicity, assume the column type in SeaTunnelDataType is the same as in phoenix
        String columnType =
                StringUtils.equalsIgnoreCase(DatabaseIdentifier.INCEPTOR, sourceCatalogName)
                        ? column.getSourceType()
                        : buildColumnType(column);
        columnSql.append(columnType);
        if (StringUtils.isNotBlank(column.getComment())) {
            columnSql.append(" COMMENT '").append(column.getComment()).append("' ");
        }
        return columnSql.toString();
    }

    private String buildColumnType(Column column) {
        return InceptorTypeConverter.INSTANCE.reconvert(column).getColumnType();
    }
}
