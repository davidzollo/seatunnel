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

package org.apache.seatunnel.engine.server.telemetry.log.operation;

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.serializable.ClientToServerOperationDataSerializerHook;
import org.apache.seatunnel.engine.server.telemetry.log.LogoutService;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class PackageJobLogsOperation extends Operation
        implements IdentifiedDataSerializable, AllowedDuringPassiveState {
    private long jobId;

    private byte[] response;

    public PackageJobLogsOperation() {}

    public PackageJobLogsOperation(long jobId) {
        this.jobId = jobId;
    }

    @Override
    public final int getFactoryId() {
        return ClientToServerOperationDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ClientToServerOperationDataSerializerHook.PACKAGE_JOB_LOGS_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(jobId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        jobId = in.readLong();
    }

    @Override
    public void run() {
        SeaTunnelServer service = getService();
        CompletableFuture<byte[]> future =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return collectAllNodesJobLogs(service);
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Failed to package job logs for jobId: " + jobId, e);
                            }
                        },
                        getNodeEngine()
                                .getExecutionService()
                                .getExecutor("package_job_logs_operation"));

        try {
            response = future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] collectAllNodesJobLogs(SeaTunnelServer service) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipOutputStream zipOut = new ZipOutputStream(baos);

        LogoutService logOutService = new LogoutService(service);
        try {
            byte[] currentNodeLogs = logOutService.packageJobLogs(jobId);

            ZipEntry currentNodeEntry =
                    new ZipEntry(
                            "node_"
                                    + getNodeEngine().getThisAddress().getHost()
                                    + "_"
                                    + getNodeEngine().getThisAddress().getPort()
                                    + "/job_"
                                    + jobId
                                    + "_logs.zip");
            zipOut.putNextEntry(currentNodeEntry);
            zipOut.write(currentNodeLogs);
            zipOut.closeEntry();
        } catch (Exception e) {
            getLogger()
                    .info(
                            String.format(
                                    "No job logs found for jobId %s on current node: %s",
                                    jobId, getNodeEngine().getThisAddress()));
        }

        for (Member member : getNodeEngine().getClusterService().getMembers()) {
            if (!member.getAddress().equals(getNodeEngine().getThisAddress())) {
                try {
                    PackageJobLogsOperation operation = new PackageJobLogsOperation(jobId);
                    Data result =
                            (Data)
                                    getNodeEngine()
                                            .getOperationService()
                                            .createInvocationBuilder(
                                                    SeaTunnelServer.SERVICE_NAME,
                                                    operation,
                                                    member.getAddress())
                                            .invoke()
                                            .get();

                    if (result != null) {
                        byte[] memberLogs =
                                getNodeEngine().getSerializationService().toObject(result);
                        if (memberLogs != null && memberLogs.length > 0) {
                            ZipEntry memberEntry =
                                    new ZipEntry(
                                            "node_"
                                                    + member.getAddress().getHost()
                                                    + "_"
                                                    + member.getAddress().getPort()
                                                    + "/job_"
                                                    + jobId
                                                    + "_logs.zip");
                            zipOut.putNextEntry(memberEntry);
                            zipOut.write(memberLogs);
                            zipOut.closeEntry();
                        }
                    }
                } catch (Exception e) {
                    getLogger()
                            .warning(
                                    "Failed to get job logs from node: "
                                            + member.getAddress()
                                            + " for jobId: "
                                            + jobId,
                                    e);
                }
            }
        }

        zipOut.close();
        return baos.toByteArray();
    }

    @Override
    public Object getResponse() {
        return response;
    }
}
