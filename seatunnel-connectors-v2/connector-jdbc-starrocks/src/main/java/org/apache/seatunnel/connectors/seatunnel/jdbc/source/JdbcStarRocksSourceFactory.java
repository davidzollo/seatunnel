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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.StarRocksIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@AutoService(Factory.class)
public class JdbcStarRocksSourceFactory extends JdbcSourceFactory {
    @Override
    public String factoryIdentifier() {
        return StarRocksIdentifier.JDBC_STARROCKS;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        Map<String, String> configMap = context.getOptions().toMap();
        configMap.put(JdbcOptions.DIALECT.key(), "StarRocks");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(new HashMap<>(configMap));
        JdbcSourceConfig config = JdbcSourceConfig.of(readonlyConfig);
        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load(
                        config.getJdbcConnectionConfig().getUrl(),
                        config.getJdbcConnectionConfig().getCompatibleMode(),
                        config.getJdbcConnectionConfig().getDialect());
        jdbcDialect.connectionUrlParse(
                config.getJdbcConnectionConfig().getUrl(),
                config.getJdbcConnectionConfig().getProperties(),
                jdbcDialect.defaultParameter());
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new JdbcSource(config);
    }
}
