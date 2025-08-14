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

package org.apache.seatunnel.engine.server.operation;

import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.concurrent.ExecutionException;

public class ListJobsByStatusOperation extends Operation implements AllowedDuringPassiveState {

    private String jobStatus;
    private Data response;

    public ListJobsByStatusOperation() {}

    public ListJobsByStatusOperation(String jobStatus) {
        this.jobStatus = jobStatus;
    }

    @Override
    public void run() {
        SeaTunnelServer service = getService();
        CompletableFuture<Data> future =
                CompletableFuture.supplyAsync(
                        () -> {
                            return this.getNodeEngine()
                                    .toData(
                                            service.getCoordinatorService()
                                                    .getJobHistoryService()
                                                    .listJobsByStatus(
                                                            JobStatus.valueOf(jobStatus)));
                        },
                        getNodeEngine()
                                .getExecutionService()
                                .getExecutor("list_jobs_by_status_operation"));
        try {
            response = future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new SeaTunnelEngineException(e);
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }
}
