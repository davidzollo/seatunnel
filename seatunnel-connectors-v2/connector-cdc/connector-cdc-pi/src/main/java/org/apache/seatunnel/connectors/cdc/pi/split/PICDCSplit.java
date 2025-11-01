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

import java.util.List;
import java.util.Objects;

/**
 * PI CDC split - only supports PI Paths
 *
 * <p>Simplified from PISplit, specifically for PI CDC real-time data split. Only uses PI Paths for
 * processing, WebIDs are not supported.
 */
public class PICDCSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final List<String> piPaths;
    private final long lastCheckpointTime;

    /**
     * Create a PI CDC split with given PI paths.
     *
     * @param splitId split identifier
     * @param piPaths list of PI paths
     */
    public PICDCSplit(String splitId, List<String> piPaths) {
        this(splitId, piPaths, 0L);
    }

    /**
     * Create a PI CDC split with given PI paths and last checkpoint time.
     *
     * @param splitId split identifier
     * @param piPaths list of PI paths
     * @param lastCheckpointTime last checkpoint time in millis
     */
    public PICDCSplit(String splitId, List<String> piPaths, long lastCheckpointTime) {

        this.splitId = splitId;
        this.piPaths = piPaths;
        this.lastCheckpointTime = lastCheckpointTime;
    }

    /**
     * Get split identifier.
     *
     * @return split id
     */
    @Override
    public String splitId() {
        return splitId;
    }

    /**
     * Get PI paths contained in this split.
     *
     * @return list of PI paths
     */
    public List<String> getPiPaths() {
        return piPaths;
    }

    /**
     * Get last checkpoint time.
     *
     * @return last checkpoint time in millis
     */
    public long getLastCheckpointTime() {
        return lastCheckpointTime;
    }

    /**
     * Get split size (number of PI paths contained).
     *
     * @return number of PI paths in this split
     */
    public int getSize() {
        return piPaths != null ? piPaths.size() : 0;
    }

    /**
     * Check if split is empty.
     *
     * @return true if no PI paths present
     */
    public boolean isEmpty() {
        return piPaths == null || piPaths.isEmpty();
    }

    @Override
    public String toString() {
        return "PICDCSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", piPathCount="
                + (piPaths != null ? piPaths.size() : 0)
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
                && Objects.equals(piPaths, that.piPaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, piPaths, lastCheckpointTime);
    }
}
