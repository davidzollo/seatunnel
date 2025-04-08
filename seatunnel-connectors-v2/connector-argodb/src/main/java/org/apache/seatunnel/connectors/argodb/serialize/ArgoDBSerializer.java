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

package org.apache.seatunnel.connectors.argodb.serialize;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import io.transwarp.holodesk.sink.ArgoDBRow;
import io.transwarp.holodesk.sink.type.NULL;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;

public class ArgoDBSerializer implements Serializable {
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final Function[] converters;

    public ArgoDBSerializer(CatalogTable table) {
        SeaTunnelRowType rowType = table.getSeaTunnelRowType();
        this.converters = new Function[rowType.getTotalFields()];
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            switch (rowType.getFieldType(i).getSqlType()) {
                case NULL:
                    converters[i] = (Function<Object, Object>) o -> NULL.value();
                    break;
                case BOOLEAN:
                case STRING:
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case DECIMAL:
                    converters[i] = (Function<Object, Object>) String::valueOf;
                    break;
                case DATE:
                    converters[i] = (Function<LocalDate, String>) o -> o.format(DATE_FORMATTER);
                    break;
                case TIME:
                    converters[i] = (Function<LocalTime, String>) o -> o.format(TIME_FORMATTER);
                    break;
                case TIMESTAMP:
                    converters[i] =
                            (Function<LocalDateTime, String>) o -> o.format(DATE_TIME_FORMATTER);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Unsupported type: %s, rowType: %s",
                                    rowType.getFieldType(i).getSqlType(), table));
            }
        }
    }

    public ArgoDBRow serialize(SeaTunnelRow seaTunnelRow) {
        Object[] fields = new Object[seaTunnelRow.getArity()];
        for (int i = 0; i < seaTunnelRow.getArity(); i++) {
            Object field = seaTunnelRow.getField(i);
            if (field == null) {
                fields[i] = NULL.value();
            } else {
                fields[i] = converters[i].apply(field);
            }
        }
        return new ArgoDBRow(fields);
    }
}
