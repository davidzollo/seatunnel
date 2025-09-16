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

package org.apache.seatunnel.connectors.cdc.pi.split;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * PI CDC checkpoint state
 *
 * <p>Used for SourceSplitEnumerator's checkpoint mechanism, managing split allocation state
 *
 * <p>Contains information about remaining and assigned splits, used for fault recovery to rebuild
 * split state
 */
public class PICDCCheckpointState implements Serializable {

    private static final long serialVersionUID = 1L;

    private long checkpointId;
    private final List<PICDCSplit> remainingSplits;
    private final List<PICDCSplit> assignedSplits;

    public PICDCCheckpointState(List<PICDCSplit> remainingSplits, List<PICDCSplit> assignedSplits) {
        this.remainingSplits = remainingSplits;
        this.assignedSplits = assignedSplits;
    }

    public List<PICDCSplit> getRemainingSplits() {
        return remainingSplits;
    }

    public List<PICDCSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public void setCheckpointId(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    @Override
    public String toString() {
        return "PICDCCheckpointState{"
                + "remainingSplits="
                + remainingSplits.size()
                + ", assignedSplits="
                + assignedSplits.size()
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PICDCCheckpointState that = (PICDCCheckpointState) o;
        return Objects.equals(remainingSplits, that.remainingSplits)
                && Objects.equals(assignedSplits, that.assignedSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(remainingSplits, assignedSplits);
    }
}
