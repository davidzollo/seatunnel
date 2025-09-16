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

package org.apache.seatunnel.connectors.seatunnel.pi.split;

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.List;
import java.util.Objects;

/**
 * PI datasplit For allocating WebIDs to different processing tasks，Supports large-scale PI data
 * source parallel processing
 */
public class PISplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final List<String> piPaths;

    private final long lastCheckpointTime;

    public PISplit(String splitId, List<String> piPaths) {
        this(splitId, piPaths, 0L);
    }

    public PISplit(String splitId, List<String> piPaths, long lastCheckpointTime) {
        this.splitId = splitId;
        this.piPaths = piPaths;
        this.lastCheckpointTime = lastCheckpointTime;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    // add compatible methods
    public String getSplitId() {
        return splitId;
    }

    public List<String> getPiPaths() {
        return piPaths;
    }

    public long getLastCheckpointTime() {
        return lastCheckpointTime;
    }

    /** Get split size (number of contained PI Paths) */
    public int getSize() {
        return piPaths != null ? piPaths.size() : 0;
    }

    /** Check if split is empty */
    public boolean isEmpty() {
        return piPaths == null || piPaths.isEmpty();
    }

    /** Create split copy with new checkpoint time */
    public PISplit withCheckpointTime(long checkpointTime) {
        return new PISplit(splitId, piPaths, checkpointTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PISplit piSplit = (PISplit) o;
        return lastCheckpointTime == piSplit.lastCheckpointTime
                && Objects.equals(splitId, piSplit.splitId)
                && Objects.equals(piPaths, piSplit.piPaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, piPaths, lastCheckpointTime);
    }

    @Override
    public String toString() {
        return "PISplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", piPathCount="
                + getSize()
                + ", lastCheckpointTime="
                + lastCheckpointTime
                + '}';
    }
}
