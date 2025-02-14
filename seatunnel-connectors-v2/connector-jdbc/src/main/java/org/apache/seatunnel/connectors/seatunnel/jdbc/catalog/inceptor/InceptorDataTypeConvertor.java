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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.inceptor;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.inceptor.InceptorTypeConverter;

import org.apache.commons.collections4.MapUtils;

import com.google.auto.service.AutoService;

import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier.INCEPTOR;

/** @deprecated instead by {@link InceptorTypeConverter} */
@Deprecated
@AutoService(DataTypeConvertor.class)
public class InceptorDataTypeConvertor implements DataTypeConvertor<String> {

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";

    public static final Integer DEFAULT_PRECISION = 38;
    public static final Integer DEFAULT_SCALE = 18;

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String dataType) {
        return toSeaTunnelType(field, dataType, Collections.emptyMap());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field, String connectorDataType, Map<String, Object> dataTypeProperties) {
        checkNotNull(connectorDataType, "Inceptor Type cannot be null");

        Long precision = MapUtils.getLong(dataTypeProperties, PRECISION);
        Integer scale = MapUtils.getInteger(dataTypeProperties, SCALE);
        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(field)
                        .nativeType(connectorDataType)
                        .dataType(connectorDataType)
                        .columnType(connectorDataType)
                        .length(precision)
                        .precision(precision)
                        .scale(scale)
                        .build();

        return InceptorTypeConverter.INSTANCE.convert(typeDefine).getDataType();
    }

    @Override
    public String toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties) {
        checkNotNull(seaTunnelDataType, "seaTunnelDataType cannot be null");
        Long precision = MapUtils.getLong(dataTypeProperties, PRECISION);
        Integer scale = MapUtils.getInteger(dataTypeProperties, SCALE);
        Column column =
                PhysicalColumn.builder()
                        .name(field)
                        .dataType(seaTunnelDataType)
                        .columnLength(precision)
                        .scale(scale)
                        .nullable(true)
                        .build();

        BasicTypeDefine typeDefine = InceptorTypeConverter.INSTANCE.reconvert(column);
        return typeDefine.getColumnType();
    }

    @Override
    public String getIdentity() {
        return INCEPTOR;
    }
}
