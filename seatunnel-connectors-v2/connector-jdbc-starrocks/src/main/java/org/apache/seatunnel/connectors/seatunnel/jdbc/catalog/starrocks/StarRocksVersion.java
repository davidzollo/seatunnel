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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.starrocks;

public enum StarRocksVersion {
    V_2,
    V_3;

    public static StarRocksVersion parse(String version) {
        if (version != null) {
            if (version.startsWith("2.")) {
                return V_2;
            }
            if (version.startsWith("3.")) {
                return V_3;
            }
        }
        throw new UnsupportedOperationException("Unsupported StarRocks version: " + version);
    }

    public boolean isBefore(StarRocksVersion version) {
        return this.compareTo(version) < 0;
    }

    public boolean isAtOrBefore(StarRocksVersion version) {
        return this.compareTo(version) <= 0;
    }

    public boolean isAfter(StarRocksVersion version) {
        return this.compareTo(version) > 0;
    }

    public boolean isAtOrAfter(StarRocksVersion version) {
        return this.compareTo(version) >= 0;
    }
}
