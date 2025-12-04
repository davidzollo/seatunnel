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

package org.apache.seatunnel.connectors.seatunnel.common.multitablesink;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;

@Getter
@EqualsAndHashCode
public class SinkIdentifier implements Serializable, Comparable<SinkIdentifier> {
    // Use jvm default serial version uid
    private static final long serialVersionUID = 8051644822115409639L;

    @JsonProperty("tableIdentifier")
    private final String tableIdentifier;

    @JsonProperty("index")
    private final int index;

    @JsonCreator
    private SinkIdentifier(
            @JsonProperty("tableIdentifier") String tableIdentifier,
            @JsonProperty("index") int index) {
        this.tableIdentifier = tableIdentifier;
        this.index = index;
    }

    public static SinkIdentifier of(String tableIdentifier, int index) {
        return new SinkIdentifier(tableIdentifier, index);
    }

    @Override
    public int compareTo(SinkIdentifier o) {
        if (o == null) {
            return 1;
        }
        int tableCompare = this.tableIdentifier.compareTo(o.tableIdentifier);
        if (tableCompare != 0) {
            return tableCompare;
        }
        return Integer.compare(this.index, o.index);
    }

    @Override
    public String toString() {
        return "{" + "tableIdentifier='" + tableIdentifier + '\'' + ", index=" + index + '}';
    }
}
