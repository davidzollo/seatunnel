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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class PackageZetaLogsOperation extends Operation
        implements IdentifiedDataSerializable, AllowedDuringPassiveState {

    private byte[] response;
    private LocalDate date;
    private String targetHost;

    /**
     * Flag to indicate if this is a sub-request to avoid recursive calls. When true, only collect
     * logs from current node without requesting other nodes.
     */
    private boolean isSubRequest;

    public PackageZetaLogsOperation() {
        this.date = LocalDate.now();
        this.targetHost = null;
        this.isSubRequest = false;
    }

    public PackageZetaLogsOperation(LocalDate date) {
        this.date = date != null ? date : LocalDate.now();
        this.targetHost = null;
        this.isSubRequest = false;
    }

    public PackageZetaLogsOperation(LocalDate date, String targetHost) {
        this.date = date != null ? date : LocalDate.now();
        this.targetHost = targetHost;
        this.isSubRequest = false;
    }

    public PackageZetaLogsOperation(LocalDate date, String targetHost, boolean isSubRequest) {
        this.date = date != null ? date : LocalDate.now();
        this.targetHost = targetHost;
        this.isSubRequest = isSubRequest;
    }

    public PackageZetaLogsOperation(String dateStr) {
        if (dateStr != null && !dateStr.trim().isEmpty()) {
            try {
                this.date = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            } catch (Exception e) {
                this.date = LocalDate.now();
            }
        } else {
            this.date = LocalDate.now();
        }
        this.targetHost = null;
        this.isSubRequest = false;
    }

    public PackageZetaLogsOperation(String dateStr, String targetHost) {
        if (dateStr != null && !dateStr.trim().isEmpty()) {
            try {
                this.date = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            } catch (Exception e) {
                this.date = LocalDate.now();
            }
        } else {
            this.date = LocalDate.now();
        }
        this.targetHost = targetHost;
        this.isSubRequest = false;
    }

    @Override
    public final int getFactoryId() {
        return ClientToServerOperationDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ClientToServerOperationDataSerializerHook.PACKAGE_ZETA_LOGS_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        out.writeString(targetHost);
        out.writeBoolean(isSubRequest);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        String dateStr = in.readString();
        if (dateStr != null && !dateStr.trim().isEmpty()) {
            try {
                this.date = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            } catch (Exception e) {
                this.date = LocalDate.now();
            }
        } else {
            this.date = LocalDate.now();
        }
        this.targetHost = in.readString();
        this.isSubRequest = in.readBoolean();
    }

    @Override
    public void run() {
        SeaTunnelServer service = getService();
        CompletableFuture<byte[]> future =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                if (targetHost == null || targetHost.trim().isEmpty()) {
                                    return collectAllNodesLogs(service);
                                } else {
                                    return collectSpecificNodeLogs(service);
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Failed to package zeta logs for date: "
                                                + date
                                                + (targetHost != null
                                                        ? ", host: " + targetHost
                                                        : ""),
                                        e);
                            }
                        },
                        getNodeEngine()
                                .getExecutionService()
                                .getExecutor("package_all_logs_operation"));

        try {
            response = future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] collectAllNodesLogs(SeaTunnelServer service) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipOutputStream zipOut = new ZipOutputStream(baos);

        LogoutService logOutService = new LogoutService(service);
        byte[] currentNodeLogs = logOutService.packageZetaLogs(date);

        ZipEntry currentNodeEntry =
                new ZipEntry(
                        "node_"
                                + getNodeEngine().getThisAddress().getHost()
                                + ":"
                                + getNodeEngine().getThisAddress().getPort()
                                + "/logs.zip");
        zipOut.putNextEntry(currentNodeEntry);
        zipOut.write(currentNodeLogs);
        zipOut.closeEntry();

        // Only collect logs from other nodes if this is not a sub-request
        // This prevents recursive calls and potential deadlock
        if (!isSubRequest) {
            for (Member member : getNodeEngine().getClusterService().getMembers()) {
                if (!member.getAddress().equals(getNodeEngine().getThisAddress())) {
                    try {
                        // Create a sub-request with isSubRequest=true to prevent recursion
                        PackageZetaLogsOperation operation =
                                new PackageZetaLogsOperation(date, null, true);
                        Object result =
                                getNodeEngine()
                                        .getOperationService()
                                        .createInvocationBuilder(
                                                SeaTunnelServer.SERVICE_NAME,
                                                operation,
                                                member.getAddress())
                                        .invoke()
                                        .get();

                        if (result != null) {
                            byte[] memberLogs = null;
                            if (result instanceof Data) {
                                memberLogs =
                                        getNodeEngine()
                                                .getSerializationService()
                                                .toObject((Data) result);
                            } else if (result instanceof byte[]) {
                                memberLogs = (byte[]) result;
                            } else {
                                getLogger()
                                        .warning(
                                                "Unexpected result type from node "
                                                        + member.getAddress()
                                                        + ": "
                                                        + result.getClass().getName());
                            }

                            if (memberLogs != null && memberLogs.length > 0) {
                                try (ZipInputStream remoteZipIn =
                                        new ZipInputStream(new ByteArrayInputStream(memberLogs))) {
                                    ZipEntry remoteEntry;
                                    byte[] buffer = new byte[8192];

                                    while ((remoteEntry = remoteZipIn.getNextEntry()) != null) {
                                        String entryName =
                                                "node_"
                                                        + member.getAddress().getHost()
                                                        + ":"
                                                        + member.getAddress().getPort()
                                                        + "/"
                                                        + remoteEntry.getName();

                                        ZipEntry newEntry = new ZipEntry(entryName);
                                        zipOut.putNextEntry(newEntry);

                                        int len;
                                        while ((len = remoteZipIn.read(buffer)) > 0) {
                                            zipOut.write(buffer, 0, len);
                                        }

                                        zipOut.closeEntry();
                                        remoteZipIn.closeEntry();
                                    }
                                } catch (IOException e) {
                                    getLogger()
                                            .warning(
                                                    "Failed to merge ZIP from node: "
                                                            + member.getAddress()
                                                            + " - "
                                                            + e.getMessage(),
                                                    e);
                                }
                            }
                        }
                    } catch (Exception e) {
                        getLogger()
                                .warning("Failed to get logs from node: " + member.getAddress(), e);
                    }
                }
            }
        }

        zipOut.close();
        return baos.toByteArray();
    }

    private byte[] collectSpecificNodeLogs(SeaTunnelServer service) throws Exception {
        String currentHost = getNodeEngine().getThisAddress().getHost();
        if (currentHost.equals(targetHost)) {
            LogoutService logOutService = new LogoutService(service);
            return logOutService.packageZetaLogs(date);
        } else {
            for (Member member : getNodeEngine().getClusterService().getMembers()) {
                if (member.getAddress().getHost().equals(targetHost)) {
                    try {
                        PackageZetaLogsOperation operation =
                                new PackageZetaLogsOperation(date, targetHost);
                        Object result =
                                getNodeEngine()
                                        .getOperationService()
                                        .createInvocationBuilder(
                                                SeaTunnelServer.SERVICE_NAME,
                                                operation,
                                                member.getAddress())
                                        .invoke()
                                        .get();

                        if (result != null) {
                            if (result instanceof Data) {
                                return getNodeEngine()
                                        .getSerializationService()
                                        .toObject((Data) result);
                            } else if (result instanceof byte[]) {
                                return (byte[]) result;
                            } else {
                                throw new RuntimeException(
                                        "Unexpected result type from target node "
                                                + targetHost
                                                + ": "
                                                + result.getClass().getName());
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Failed to get logs from target node: " + targetHost, e);
                    }
                }
            }
            throw new RuntimeException("Target node not found: " + targetHost);
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }
}
