package org.apache.seatunnel.connectors.argodb.serialize;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import io.transwarp.holodesk.sink.ArgoDBRow;
import io.transwarp.holodesk.sink.type.NULL;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ArgoDBSerializerTest {

    private ArgoDBSerializer serializer;
    private CatalogTable table;

    @Test
    void serializeWithNullValues() {
        table =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.builder()
                                                .name("field")
                                                .dataType(BasicType.VOID_TYPE)
                                                .build())
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null,
                        null);
        serializer = new ArgoDBSerializer(table);

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {null});
        ArgoDBRow result = serializer.serialize(row);

        assertEquals(NULL.value(), result.getRow()[0]);
    }

    @Test
    void serializeWithStringValues() {
        table =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.builder()
                                                .name("field")
                                                .dataType(BasicType.STRING_TYPE)
                                                .build())
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null,
                        null);
        serializer = new ArgoDBSerializer(table);

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"test"});
        ArgoDBRow result = serializer.serialize(row);

        assertEquals("test", result.getRow()[0]);
    }

    @Test
    void serializeWithDateValues() {
        table =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.builder()
                                                .name("field")
                                                .dataType(LocalTimeType.LOCAL_DATE_TYPE)
                                                .build())
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null,
                        null);
        serializer = new ArgoDBSerializer(table);

        LocalDate date = LocalDate.of(2023, 10, 1);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {date});
        ArgoDBRow result = serializer.serialize(row);

        assertEquals("2023-10-01", result.getRow()[0]);
    }

    @Test
    void serializeWithTimeValues() {
        table =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.builder()
                                                .name("field")
                                                .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                                                .build())
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null,
                        null);
        serializer = new ArgoDBSerializer(table);

        LocalTime time = LocalTime.of(12, 30, 45);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {time});
        ArgoDBRow result = serializer.serialize(row);

        assertEquals("12:30:45", result.getRow()[0]);
    }

    @Test
    void serializeWithTimestampValues() {
        table =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.builder()
                                                .name("field")
                                                .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                                                .build())
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null,
                        null);
        serializer = new ArgoDBSerializer(table);

        LocalDateTime dateTime = LocalDateTime.of(2023, 10, 1, 12, 30, 45);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {dateTime});
        ArgoDBRow result = serializer.serialize(row);

        assertEquals("2023-10-01 12:30:45", result.getRow()[0]);
    }

    @Test
    void serializeWithUnsupportedType() {
        table =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.builder()
                                                .name("field")
                                                .dataType(ArrayType.INT_ARRAY_TYPE)
                                                .build())
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null,
                        null);
        assertThrows(UnsupportedOperationException.class, () -> new ArgoDBSerializer(table));
    }
}
