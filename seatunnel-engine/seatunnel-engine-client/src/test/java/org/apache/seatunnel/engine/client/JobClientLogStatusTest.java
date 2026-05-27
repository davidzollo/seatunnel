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

import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.client.job.JobLogContent;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JobClientLogStatusTest {
    private static final Long JOB_ID = 123L;

    @Test
    public void testParseJobLogContentReturnsFileNotFoundStatus() throws Exception {
        JobLogContent jobLogContent =
                parseJobLogContent(
                        createOuterZipWithNestedZip(
                                "node_127.0.0.1:5801/job_123_logs.zip",
                                "job_123/.job-log-status",
                                "FILE_NOT_FOUND"));

        assertEquals(JobLogContent.LogStatus.FILE_NOT_FOUND, jobLogContent.getLogStatus());
        assertTrue(jobLogContent.getNodeLogs().isEmpty());
        assertTrue(jobLogContent.getStatusMessage().contains("123"));
    }

    @Test
    public void testParseJobLogContentKeepsEmptyFileAsAvailable() throws Exception {
        JobLogContent jobLogContent =
                parseJobLogContent(
                        createOuterZipWithNestedZip(
                                "node_127.0.0.1:5801/job_123_logs.zip", "job_123/job-123.log", ""));

        assertEquals(JobLogContent.LogStatus.AVAILABLE, jobLogContent.getLogStatus());
        assertEquals(1, jobLogContent.getNodeLogs().size());
        assertEquals("", jobLogContent.getNodeLogs().get(0).getLog());
    }

    private JobLogContent parseJobLogContent(byte[] logBytes)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        JobClient jobClient = new JobClient(Mockito.mock(SeaTunnelHazelcastClient.class));
        Method parseMethod =
                JobClient.class.getDeclaredMethod("parseJobLogContent", byte[].class, Long.class);
        parseMethod.setAccessible(true);
        return (JobLogContent) parseMethod.invoke(jobClient, logBytes, JOB_ID);
    }

    private byte[] createOuterZipWithNestedZip(
            String outerEntryName, String nestedEntryName, String nestedEntryContent)
            throws IOException {
        byte[] nestedZip = createZipEntry(nestedEntryName, nestedEntryContent);
        return createZipEntry(outerEntryName, nestedZip);
    }

    private byte[] createZipEntry(String entryName, String entryContent) throws IOException {
        return createZipEntry(entryName, entryContent.getBytes(StandardCharsets.UTF_8));
    }

    private byte[] createZipEntry(String entryName, byte[] entryContent) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ZipOutputStream zipOut = new ZipOutputStream(baos)) {
            zipOut.putNextEntry(new ZipEntry(entryName));
            zipOut.write(entryContent);
            zipOut.closeEntry();
            zipOut.finish();
            return baos.toByteArray();
        }
    }
}
