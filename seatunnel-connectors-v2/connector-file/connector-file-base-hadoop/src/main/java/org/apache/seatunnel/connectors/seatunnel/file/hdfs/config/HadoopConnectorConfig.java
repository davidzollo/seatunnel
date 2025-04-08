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

package org.apache.seatunnel.connectors.seatunnel.file.hdfs.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

public class HadoopConnectorConfig extends HadoopConf {

    public HadoopConnectorConfig(String hdfsNameKey) {
        super(hdfsNameKey);
    }

    public static HadoopConf buildWithConfig(ReadonlyConfig config) {
        HadoopConf hadoopConf = new HadoopConnectorConfig(config.get(HdfsConfigOptions.DEFAULT_FS));
        if (config.getOptional(HdfsConfigOptions.HDFS_SITE_PATH).isPresent()) {
            hadoopConf.setHdfsSitePath(config.get(HdfsConfigOptions.HDFS_SITE_PATH));
        }
        if (config.getOptional(HdfsConfigOptions.HADOOP_CONF_PATH).isPresent()) {
            hadoopConf.setHadoopConfPath(config.get(HdfsConfigOptions.HADOOP_CONF_PATH));
        }

        if (config.getOptional(HdfsConfigOptions.REMOTE_USER).isPresent()) {
            hadoopConf.setRemoteUser(config.get(HdfsConfigOptions.REMOTE_USER));
        }

        if (config.getOptional(HdfsConfigOptions.KRB5_PATH).isPresent()) {
            hadoopConf.setKrb5Path(config.get(HdfsConfigOptions.KRB5_PATH));
        }

        if (config.getOptional(HdfsConfigOptions.KERBEROS_PRINCIPAL).isPresent()) {
            hadoopConf.setKerberosPrincipal(config.get(HdfsConfigOptions.KERBEROS_PRINCIPAL));
        }

        if (config.getOptional(HdfsConfigOptions.KERBEROS_KEYTAB_PATH).isPresent()) {
            hadoopConf.setKerberosKeytabPath(config.get(HdfsConfigOptions.KERBEROS_KEYTAB_PATH));
        }

        return hadoopConf;
    }
}
