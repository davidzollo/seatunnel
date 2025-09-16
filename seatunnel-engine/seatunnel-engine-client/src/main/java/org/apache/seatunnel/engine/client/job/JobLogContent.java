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

package org.apache.seatunnel.engine.client.job;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/** Job log content object containing host and log information */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobLogContent {

    /** List of log entries from different nodes */
    private List<NodeLogEntry> nodeLogs;

    /** Node log entry containing host and log content */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NodeLogEntry {
        private String host;
        private String log;
    }
}
