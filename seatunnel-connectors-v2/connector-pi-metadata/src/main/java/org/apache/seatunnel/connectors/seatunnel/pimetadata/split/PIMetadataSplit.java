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

package org.apache.seatunnel.connectors.seatunnel.pimetadata.split;

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PIMetadataSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private String splitId;
    private List<String> piPaths;
    private int batchIndex;
    private boolean isCompleted = false;

    public PIMetadataSplit(String splitId, List<String> piPaths, int batchIndex) {
        this.splitId = splitId;
        this.piPaths = piPaths;
        this.batchIndex = batchIndex;
        this.isCompleted = false;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public int getPathCount() {
        return piPaths != null ? piPaths.size() : 0;
    }

    public void markCompleted() {
        this.isCompleted = true;
    }

    public boolean isCompleted() {
        return isCompleted;
    }

    public int getBatchIndex() {
        return batchIndex;
    }

    public List<String> getPiPaths() {
        return piPaths;
    }

    @Override
    public String toString() {
        return "PIMetadataSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", piPaths="
                + piPaths
                + ", batchIndex="
                + batchIndex
                + ", isCompleted="
                + isCompleted
                + '}';
    }
}
