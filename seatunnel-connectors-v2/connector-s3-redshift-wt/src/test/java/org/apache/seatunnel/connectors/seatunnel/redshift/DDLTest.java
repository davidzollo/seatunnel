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

package org.apache.seatunnel.connectors.seatunnel.redshift;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;
import org.apache.seatunnel.connectors.seatunnel.redshift.sink.S3RedshiftChangelogWriter;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Disabled("we have no redshift env for test")
public class DDLTest {

    @Test
    public void testModifyTable() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"f1", "f2"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

        WriteStrategy strategy =
                WriteStrategyFactory.of(
                        FileFormat.PARQUET, new FileSinkConfig(ConfigFactory.empty(), rowType));
        strategy.setSeaTunnelRowTypeInfo(rowType);

        // "jdbc_url":
        // "jdbc:redshift://redshift-cluster-1.c4bak5qvmy8r.cn-north-1.redshift.amazonaws.com.cn:5439/dev",
        // "bucket": "s3a://ws-package",
        // "fs.s3a.endpoint": "s3.cn-north-1.amazonaws.com.cn",
        // "fs.s3a.aws.credentials.provider":
        // "com.amazonaws.auth.InstanceProfileCredentialsProvider",
        // "access_key": "AKIAYYUV5DMXNWIDLUEB",
        // "secret_key": "AWS_XXXX",
        // "jdbc_user": "awsuser",
        // "jdbc_password": "Whaleops123!",
        S3RedshiftConf conf =
                S3RedshiftConf.builder()
                        .jdbcUrl(
                                "jdbc:redshift://redshift-cluster-1.c4bak5qvmy8r.cn-north-1.redshift.amazonaws.com.cn:5439/dev")
                        .s3Bucket("s3a://ws-package")
                        .redshiftTemporaryTableName("st_temporary_${redshift_table}")
                        .redshiftTablePrimaryKeys(Collections.singletonList("f1"))
                        .redshiftTable("public.test")
                        .accessKey("AKIAYYUV5DMXNWIDLUEB")
                        .jdbcUser("awsuser")
                        .jdbcPassword("Whaleops123!")
                        .build();

        S3RedshiftChangelogWriter writer =
                new S3RedshiftChangelogWriter(
                        strategy, null, null, "1", new ArrayList<>(), rowType, conf);
        List<AlterTableColumnEvent> alterTableColumnEvents = new ArrayList<>();
        AlterTableAddColumnEvent e1 =
                new AlterTableAddColumnEvent(
                        TableIdentifier.of(null, TablePath.of("t.t")),
                        PhysicalColumn.of("f3", BasicType.STRING_TYPE, 0, false, null, ""),
                        true,
                        null);
        alterTableColumnEvents.add(e1);

        AlterTableAddColumnEvent e2 =
                new AlterTableChangeColumnEvent(
                        TableIdentifier.of(null, TablePath.of("t.t")),
                        "f2",
                        PhysicalColumn.of("f4", BasicType.STRING_TYPE, 0, false, null, ""),
                        true,
                        null);
        alterTableColumnEvents.add(e2);

        AlterTableDropColumnEvent e3 =
                new AlterTableDropColumnEvent(TableIdentifier.of(null, TablePath.of("t.t")), "f3");
        alterTableColumnEvents.add(e3);

        AlterTableColumnsEvent events =
                new AlterTableColumnsEvent(
                        TableIdentifier.of(null, TablePath.of("t.t")), alterTableColumnEvents);
        writer.applySchemaChange(events);
    }
}
