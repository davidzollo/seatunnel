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

package org.apache.seatunnel.engine.server.dag;

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.dag.actions.ActionUtils;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.Edge;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.VertexInfo;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionPlanGenerator;
import org.apache.seatunnel.engine.server.dag.execution.Pipeline;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DAGUtils {

    public static LogicalDag restoreLogicalDag(
            JobImmutableInformation jobImmutableInformation,
            SerializationService serializationService,
            List<ClassLoader> classLoaders) {
        LogicalDag logicalDag =
                serializationService.toObject(jobImmutableInformation.getLogicalDag());
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            List<Data> logicalVertexDataList = jobImmutableInformation.getLogicalVertexDataList();
            for (int i = 0; i < jobImmutableInformation.getLogicalVertexDataList().size(); i++) {
                Thread.currentThread().setContextClassLoader(classLoaders.get(i));
                logicalDag.addLogicalVertex(
                        serializationService.toObject(logicalVertexDataList.get(i)));
            }
            return logicalDag;
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
    }

    public static LogicalDag restoreLogicalDag(
            JobImmutableInformation jobImmutableInformation,
            SerializationService serializationService,
            ClassLoaderService classLoaderService) {
        List<Set<URL>> logicalVertexJarsList = jobImmutableInformation.getLogicalVertexJarsList();
        List<ClassLoader> classLoaders = new ArrayList<>();
        try {
            for (Set<URL> urls : logicalVertexJarsList) {
                classLoaders.add(
                        classLoaderService.getClassLoader(
                                jobImmutableInformation.getJobId(), urls));
            }
            return restoreLogicalDag(jobImmutableInformation, serializationService, classLoaders);
        } finally {
            for (Set<URL> urls : logicalVertexJarsList) {
                classLoaderService.releaseClassLoader(jobImmutableInformation.getJobId(), urls);
            }
        }
    }

    public static JobDAGInfo getJobDAGInfo(
            LogicalDag logicalDag,
            JobImmutableInformation jobImmutableInformation,
            EngineConfig engineConfig,
            boolean isPhysicalDAGIInfo) {
        List<Pipeline> pipelines =
                new ExecutionPlanGenerator(logicalDag, jobImmutableInformation, engineConfig)
                        .generate()
                        .getPipelines();
        if (isPhysicalDAGIInfo) {
            // Generate ExecutePlan DAG
            Map<Integer, List<Edge>> pipelineWithEdges = new HashMap<>();
            Map<Long, VertexInfo> vertexInfoMap = new HashMap<>();
            pipelines.forEach(
                    pipeline -> {
                        pipelineWithEdges.put(
                                pipeline.getId(),
                                pipeline.getEdges().stream()
                                        .map(
                                                e ->
                                                        new Edge(
                                                                e.getLeftVertexId(),
                                                                e.getRightVertexId()))
                                        .collect(Collectors.toList()));
                        pipeline.getVertexes()
                                .forEach(
                                        (id, vertex) -> {
                                            vertexInfoMap.put(
                                                    id,
                                                    new VertexInfo(
                                                            vertex.getVertexId(),
                                                            ActionUtils.getActionType(
                                                                    vertex.getAction()),
                                                            vertex.getAction().getName()));
                                        });
                    });
            return new JobDAGInfo(
                    jobImmutableInformation.getJobId(), pipelineWithEdges, vertexInfoMap);
        } else {
            // Generate LogicalPlan DAG
            List<Edge> edges =
                    logicalDag.getEdges().stream()
                            .map(e -> new Edge(e.getInputVertexId(), e.getTargetVertexId()))
                            .collect(Collectors.toList());

            Map<Long, LogicalVertex> logicalVertexMap = logicalDag.getLogicalVertexMap();
            Map<Long, VertexInfo> vertexInfoMap =
                    logicalVertexMap.values().stream()
                            .map(
                                    v ->
                                            new VertexInfo(
                                                    v.getVertexId(),
                                                    ActionUtils.getActionType(v.getAction()),
                                                    v.getAction().getName()))
                            .collect(
                                    Collectors.toMap(VertexInfo::getVertexId, Function.identity()));

            Map<Integer, List<Edge>> pipelineWithEdges =
                    edges.stream()
                            .collect(
                                    Collectors.groupingBy(
                                            e -> {
                                                LogicalVertex info =
                                                        logicalVertexMap.get(
                                                                e.getInputVertexId() != null
                                                                        ? e.getInputVertexId()
                                                                        : e.getTargetVertexId());
                                                return pipelines.stream()
                                                        .filter(
                                                                p ->
                                                                        p.getActions()
                                                                                .containsKey(
                                                                                        info.getAction()
                                                                                                .getId()))
                                                        .findFirst()
                                                        .get()
                                                        .getId();
                                            },
                                            Collectors.toList()));
            return new JobDAGInfo(
                    jobImmutableInformation.getJobId(), pipelineWithEdges, vertexInfoMap);
        }
    }
}
