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

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * PI CDC split
 *
 * <p>Simplified from PISplit, specifically for PI CDC real-time data split
 */
public class PICDCSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final List<String> piPaths;
    private final List<String> webIds;

    private final long lastCheckpointTime;

    public PICDCSplit(String splitId, List<String> piPaths, List<String> webIds) {
        this(splitId, piPaths, webIds, 0L);
    }

    public PICDCSplit(
            String splitId, List<String> piPaths, List<String> webIds, long lastCheckpointTime) {
        this.splitId = splitId;
        this.piPaths = piPaths;
        this.webIds = webIds;
        this.lastCheckpointTime = lastCheckpointTime;
    }

    /**
     * Simplified constructor - consistent with PISplit
     *
     * @param splitId split ID
     * @param webIds WebID list (can be PI Path or WebID)
     */
    public PICDCSplit(String splitId, List<String> webIds) {
        this(splitId, new ArrayList<>(), webIds, 0L);
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public List<String> getPiPaths() {
        return piPaths;
    }

    public List<String> getWebIds() {
        return webIds;
    }

    public long getLastCheckpointTime() {
        return lastCheckpointTime;
    }

    /** Get split size (number of WebIDs contained) - consistent with PISplit */
    public int getSize() {
        int piPathCount = piPaths != null ? piPaths.size() : 0;
        int webIdCount = webIds != null ? webIds.size() : 0;
        return piPathCount + webIdCount;
    }

    /** Check if split is empty - consistent with PISplit */
    public boolean isEmpty() {
        boolean piPathsEmpty = piPaths == null || piPaths.isEmpty();
        boolean webIdsEmpty = webIds == null || webIds.isEmpty();
        return piPathsEmpty && webIdsEmpty;
    }

    /** Create a split copy with new checkpoint time */
    public PICDCSplit withCheckpointTime(long checkpointTime) {
        return new PICDCSplit(splitId, piPaths, webIds, checkpointTime);
    }

    /** CDC split recovery - create new split with updated start time */
    public PICDCSplit withStartTime(long startTime) {
        return new PICDCSplit(splitId, piPaths, webIds, startTime);
    }

    /** Get start time for CDC processing */
    public long getStartTime() {
        return lastCheckpointTime;
    }

    @Override
    public String toString() {
        return "PICDCSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", piPathCount="
                + (piPaths != null ? piPaths.size() : 0)
                + ", webIdCount="
                + (webIds != null ? webIds.size() : 0)
                + ", lastCheckpointTime="
                + lastCheckpointTime
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PICDCSplit)) return false;
        PICDCSplit that = (PICDCSplit) o;
        return lastCheckpointTime == that.lastCheckpointTime
                && Objects.equals(splitId, that.splitId)
                && Objects.equals(piPaths, that.piPaths)
                && Objects.equals(webIds, that.webIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, piPaths, webIds, lastCheckpointTime);
    }
}
