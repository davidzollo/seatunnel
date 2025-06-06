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

package org.apache.seatunnel.connectors.seatunnel.sapbw.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.connectors.seatunnel.sapbw.client.SAPJcoClient;
import org.apache.seatunnel.connectors.seatunnel.sapbw.config.QueryTableConfig;
import org.apache.seatunnel.connectors.seatunnel.sapbw.config.SAPBWSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.sapbw.state.SAPBWState;

import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoTable;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class SAPBWSplitEnumerator implements SourceSplitEnumerator<SAPBWSplit, SAPBWState> {
    private final SourceSplitEnumerator.Context<SAPBWSplit> enumeratorContext;
    private final Map<Integer, Set<SAPBWSplit>> pendingSplits;

    private final SAPBWSourceConfig sapbwSourceConfig;
    private final Set<SAPBWSplit> assignedSplits;
    private final Object lock = new Object();
    private SAPJcoClient client;

    public SAPBWSplitEnumerator(
            SourceSplitEnumerator.Context<SAPBWSplit> enumeratorContext,
            SAPBWSourceConfig sapbwSourceConfig,
            Set<SAPBWSplit> assignedSplits) {
        this.enumeratorContext = enumeratorContext;
        this.pendingSplits = new HashMap<>();
        this.sapbwSourceConfig = sapbwSourceConfig;
        this.assignedSplits = new HashSet<>(assignedSplits);
    }

    @Override
    public void open() {
        client = SAPJcoClient.createClient(sapbwSourceConfig);
    }

    @Override
    public void run() throws Exception {
        discoverySplits();
        assignPendingSplits();
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                throw new CatalogException("Failed to close SAP BW client", e);
            } finally {
                client = null;
            }
        }
    }

    @Override
    public void addSplitsBack(List<SAPBWSplit> splits, int subtaskId) {
        log.debug("Add splits back to pending assignment {}", splits);
        addSplitChangeToPendingAssignments(splits);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {
        // nothing
    }

    @Override
    public SAPBWState snapshotState(long checkpointId) throws Exception {
        synchronized (lock) {
            return new SAPBWState(assignedSplits);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    private void discoverySplits() throws JCoException {
        Set<SAPBWSplit> allSplit = new HashSet<>();
        for (Map.Entry<TablePath, QueryTableConfig> entry :
                sapbwSourceConfig.getQueryTableConfigs().entrySet()) {
            QueryTableConfig tableConfig = entry.getValue();
            TablePath tablePath = entry.getKey();
            JCoFunction getMembers =
                    client.getDestination()
                            .getRepository()
                            .getFunction("BAPI_MDPROVIDER_GET_MEMBERS");
            getMembers.getImportParameterList().setValue("CAT_NAM", tablePath.getDatabaseName());
            getMembers.getImportParameterList().setValue("CUBE_NAM", tablePath.getTableName());
            getMembers.execute(client.getDestination());
            JCoTable members = getMembers.getTableParameterList().getTable("MEMBERS");
            List<String> memberIds = new ArrayList<>();
            for (int i = 0; i < members.getNumRows(); i++) {
                members.setRow(i);
                String memberName = members.getString("MEM_UNAM");
                // filter aggregate member and not selected dimension
                if (!memberName.startsWith("[Measures]")
                        && members.getInt("CHILDREN") == 0
                        && (tableConfig.getDimensionsAndMeasures() == null
                                || tableConfig.getDimensionsAndMeasures().isEmpty()
                                || tableConfig
                                        .getDimensionsAndMeasures()
                                        .contains(memberNameToDimension(memberName)))) {
                    memberIds.add(memberName);
                }
            }
            Optional<Map.Entry<String, List<String>>> maxEntry =
                    memberIds.stream()
                            .collect(Collectors.groupingBy(this::memberNameToDimension))
                            .entrySet()
                            .stream()
                            .max(Comparator.comparingInt(e -> e.getValue().size()));
            if (maxEntry.isPresent()) {
                List<String> membersList = maxEntry.get().getValue();
                for (List<String> memberSplit :
                        splitListEvenly(membersList, enumeratorContext.currentParallelism())) {
                    allSplit.add(new SAPBWSplit(tablePath, memberSplit));
                }
            } else {
                allSplit.add(new SAPBWSplit(tablePath));
            }
        }

        assignedSplits.forEach(allSplit::remove);
        addSplitChangeToPendingAssignments(allSplit);
        log.info("Calculated splits successfully, the size of splits is {}.", allSplit.size());
    }

    private String memberNameToDimension(String memberName) {
        return memberName.substring(1, memberName.lastIndexOf(".") - 1);
    }

    public static List<List<String>> splitListEvenly(List<String> original, int parts) {
        List<List<String>> result = new ArrayList<>();
        int totalSize = original.size();
        int baseSize = totalSize / parts;
        int remainder = totalSize % parts;

        int fromIndex = 0;
        for (int i = 0; i < parts; i++) {
            int currentSize = baseSize + (i < remainder ? 1 : 0);
            int toIndex = fromIndex + currentSize;
            if (fromIndex >= totalSize) {
                result.add(Collections.emptyList());
            } else {
                result.add(
                        new ArrayList<>(original.subList(fromIndex, Math.min(toIndex, totalSize))));
            }
            fromIndex = toIndex;
        }
        return result;
    }

    private void addSplitChangeToPendingAssignments(Collection<SAPBWSplit> newSplits) {
        for (SAPBWSplit split : newSplits) {
            int ownerReader =
                    (split.hashCode() & Integer.MAX_VALUE) % enumeratorContext.currentParallelism();
            pendingSplits.computeIfAbsent(ownerReader, r -> new HashSet<>()).add(split);
        }
    }

    private void assignPendingSplits() {
        // Check if there's any pending splits for given readers
        for (int pendingReader : enumeratorContext.registeredReaders()) {
            // Remove pending assignment for the reader
            final Set<SAPBWSplit> pendingAssignmentForReader = pendingSplits.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                // Mark pending splits as already assigned
                synchronized (lock) {
                    assignedSplits.addAll(pendingAssignmentForReader);
                    // Assign pending splits to reader
                    log.info(
                            "Assigning splits to readers {} {}",
                            pendingReader,
                            pendingAssignmentForReader);
                    enumeratorContext.assignSplit(
                            pendingReader, new ArrayList<>(pendingAssignmentForReader));
                }
            }
            enumeratorContext.signalNoMoreSplits(pendingReader);
        }
    }
}
