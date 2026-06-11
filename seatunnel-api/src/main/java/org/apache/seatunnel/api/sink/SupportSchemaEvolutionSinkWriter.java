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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;

import java.io.IOException;
import java.util.Optional;

public interface SupportSchemaEvolutionSinkWriter {

    /**
     * apply schema change to third party data receiver.
     *
     * @param event
     * @throws IOException
     */
    void applySchemaChange(SchemaChangeEvent event) throws IOException;

    /**
     * Returns a stable identifier of the physical sink table this writer commits to. Multi-table
     * sinks that resolve a sink-table template per upstream table (e.g. JDBC sink with a {@code
     * cdc${table_name}} template) can end up with several writers that share one physical
     * destination table; when that happens a schema change applied to one writer mutates the
     * underlying database immediately, while the sibling writers keep their in-memory output format
     * pointing at the old schema and start failing on the next commit (issue #4252).
     *
     * <p>Writers that can co-share a physical destination should expose the resolved physical table
     * identifier here. The multi-table coordinator uses this identifier to detect overlap and
     * broadcast schema-change events to every co-sharing writer so their in-memory output format
     * stays in sync with the actual database schema. The default implementation returns {@link
     * Optional#empty()} which preserves the legacy strictly-per-source-table routing for connectors
     * that do not yet opt in.
     */
    default Optional<String> getPhysicalSinkTableIdentifier() {
        return Optional.empty();
    }
}
