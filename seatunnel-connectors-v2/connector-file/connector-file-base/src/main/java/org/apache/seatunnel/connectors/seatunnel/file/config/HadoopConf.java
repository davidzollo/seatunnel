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

package org.apache.seatunnel.connectors.seatunnel.file.config;

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;
import static org.apache.parquet.avro.AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE;

@Data
@Slf4j
public class HadoopConf implements Serializable {
    private static final List<String> HADOOP_CONF_FILES =
            ImmutableList.of("hdfs-site.xml", "core-site.xml");
    private static final String HDFS_IMPL = "org.apache.hadoop.hdfs.DistributedFileSystem";
    private static final String SCHEMA = "hdfs";
    protected Map<String, String> extraOptions = new HashMap<>();
    protected String hdfsNameKey;
    protected String hdfsSitePath;
    protected String hadoopConfPath;

    protected String remoteUser;

    private String krb5Path;
    protected String kerberosPrincipal;
    protected String kerberosKeytabPath;

    public HadoopConf(String hdfsNameKey) {
        this.hdfsNameKey = hdfsNameKey;
    }

    public String getFsHdfsImpl() {
        return HDFS_IMPL;
    }

    public String getSchema() {
        return SCHEMA;
    }

    public void setExtraOptionsForConfiguration(Configuration configuration) {
        if (!extraOptions.isEmpty()) {
            extraOptions.forEach(configuration::set);
        }
        try {
            if (StringUtils.isNotBlank(hdfsSitePath)) {
                configuration.addResource(new File(hdfsSitePath).toURI().toURL());
            }
            if (StringUtils.isNotBlank(hadoopConfPath)) {
                HADOOP_CONF_FILES.forEach(
                        confFile -> {
                            java.nio.file.Path path = Paths.get(hadoopConfPath, confFile);
                            if (Files.exists(path)) {
                                try {
                                    configuration.addResource(path.toUri().toURL());
                                } catch (IOException e) {
                                    log.warn(
                                            "Error adding Hadoop resource {}, resource was not added",
                                            path,
                                            e);
                                }
                            }
                        });
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException("Add hdfs site failed: ", e);
        }
    }

    public Configuration toConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setBoolean(READ_INT96_AS_FIXED, true);
        configuration.setBoolean(ADD_LIST_ELEMENT_RECORDS, false);
        configuration.setBoolean(WRITE_OLD_LIST_STRUCTURE, true);
        configuration.setBoolean(String.format("fs.%s.impl.disable.cache", getSchema()), true);
        configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, getHdfsNameKey());
        configuration.set(String.format("fs.%s.impl", getSchema()), getFsHdfsImpl());
        return configuration;
    }
}
