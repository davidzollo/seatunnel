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

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/** Job log content object containing host and log information */
@Data
@NoArgsConstructor
public class JobLogContent {

    /** List of log entries from different nodes */
    private List<NodeLogEntry> nodeLogs;

    /** Overall job log query status */
    private LogStatus logStatus = LogStatus.AVAILABLE;

    /** Human-readable status message for non-available responses */
    private String statusMessage;

    public JobLogContent(List<NodeLogEntry> nodeLogs, LogStatus logStatus, String statusMessage) {
        this.nodeLogs = nodeLogs;
        this.logStatus = logStatus;
        this.statusMessage = statusMessage;
    }

    public JobLogContent(List<NodeLogEntry> nodeLogs) {
        this(nodeLogs, LogStatus.AVAILABLE, null);
    }

    /** Overall job log query status */
    public enum LogStatus {
        AVAILABLE,
        FILE_NOT_FOUND
    }

    /** Node log entry containing host and log content */
    @Data
    @NoArgsConstructor
    public static class NodeLogEntry {
        private String host;
        private String log;

        public NodeLogEntry(String host, String log) {
            this.host = host;
            this.log = log;
        }
    }
}
