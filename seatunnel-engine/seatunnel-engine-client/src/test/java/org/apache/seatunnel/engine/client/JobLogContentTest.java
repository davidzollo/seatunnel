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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.engine.client.job.JobLogContent;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class JobLogContentTest {

    @Test
    public void testJobLogContentCreation() {
        JobLogContent.NodeLogEntry nodeLogEntry =
                new JobLogContent.NodeLogEntry("192.168.1.100:5801", "test log content");

        assertNotNull(nodeLogEntry);
        assertEquals("192.168.1.100:5801", nodeLogEntry.getHost());
        assertEquals("test log content", nodeLogEntry.getLog());
    }

    @Test
    public void testJobLogContentWithNodeLogs() {
        JobLogContent.NodeLogEntry nodeLogEntry1 =
                new JobLogContent.NodeLogEntry("192.168.1.100:5801", "log content from node 1");
        JobLogContent.NodeLogEntry nodeLogEntry2 =
                new JobLogContent.NodeLogEntry("192.168.1.101:5801", "log content from node 2");

        JobLogContent jobLogContent =
                new JobLogContent(Arrays.asList(nodeLogEntry1, nodeLogEntry2));

        assertNotNull(jobLogContent);
        assertEquals(2, jobLogContent.getNodeLogs().size());
    }
}
