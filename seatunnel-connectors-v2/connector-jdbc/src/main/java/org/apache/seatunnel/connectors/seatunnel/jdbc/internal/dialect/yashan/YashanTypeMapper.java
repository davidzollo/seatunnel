package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashan;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class YashanTypeMapper implements JdbcDialectTypeMapper {

    private final boolean decimalTypeNarrowing;

    public YashanTypeMapper() {
        this(JdbcOptions.DECIMAL_TYPE_NARROWING.defaultValue());
    }

    public YashanTypeMapper(boolean decimalTypeNarrowing) {
        this.decimalTypeNarrowing = decimalTypeNarrowing;
    }

    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return new YashanTypeConverter(decimalTypeNarrowing).convert(typeDefine);
    }

    @Override
    public Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnName = metadata.getColumnLabel(colIndex);
        String nativeType = metadata.getColumnTypeName(colIndex);
        int isNullable = metadata.isNullable(colIndex);
        long precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        if ("number".equalsIgnoreCase(nativeType) && scale == -127) {
            nativeType = "float";
        } else if ("NCHAR".equalsIgnoreCase(nativeType)) {
            precision = TypeDefineUtils.charToDoubleByteLength(precision);
        }

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(nativeType)
                        .dataType(nativeType)
                        .nullable(isNullable == ResultSetMetaData.columnNullable)
                        .length(precision)
                        .precision(precision)
                        .scale(scale)
                        .build();
        return mappingColumn(typeDefine);
    }
}
