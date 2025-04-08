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

package io.debezium.connector.oracle.logminer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.Scn;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SeaTunnelLogMinerHelper extends LogMinerHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(SeaTunnelLogMinerHelper.class);

    /**
     * Overridden method ${@link LogMinerHelper#setLogFilesForMining(OracleConnection, Scn,
     * Duration, boolean, String)} to return the list of log files.
     */
    public static List<String> setLogFilesForMiningWithResult(
            OracleConnection connection,
            Scn lastProcessedScn,
            Duration archiveLogRetention,
            boolean archiveLogOnlyMode,
            String archiveDestinationName)
            throws SQLException {
        removeLogFilesFromMining(connection);

        List<LogFile> logFilesForMining =
                getLogFilesForOffsetScn(
                        connection,
                        lastProcessedScn,
                        archiveLogRetention,
                        archiveLogOnlyMode,
                        archiveDestinationName);
        if (!logFilesForMining.stream()
                .anyMatch(l -> l.getFirstScn().compareTo(lastProcessedScn) <= 0)) {
            Scn minScn =
                    logFilesForMining.stream()
                            .map(LogFile::getFirstScn)
                            .min(Scn::compareTo)
                            .orElse(Scn.NULL);

            if ((minScn.isNull() || logFilesForMining.isEmpty()) && archiveLogOnlyMode) {
                throw new DebeziumException(
                        "The log.mining.archive.log.only mode was recently enabled and the offset SCN "
                                + lastProcessedScn
                                + "is not yet in any available archive logs. "
                                + "Please perform an Oracle log switch and restart the connector.");
            }
            throw new IllegalStateException(
                    "None of log files contains offset SCN: "
                            + lastProcessedScn
                            + ", re-snapshot is required.");
        }

        List<String> logFilesNames =
                logFilesForMining.stream().map(LogFile::getFileName).collect(Collectors.toList());
        for (String file : logFilesNames) {
            LOGGER.trace("Adding log file {} to mining session", file);
            String addLogFileStatement = SqlUtils.addLogFileStatement("DBMS_LOGMNR.ADDFILE", file);
            executeCallableStatement(connection, addLogFileStatement);
        }

        LOGGER.debug(
                "Last mined SCN: {}, Log file list to mine: {}\n", lastProcessedScn, logFilesNames);
        return logFilesNames;
    }

    private static void executeCallableStatement(OracleConnection connection, String statement)
            throws SQLException {
        Objects.requireNonNull(statement);
        try (CallableStatement s = connection.connection(false).prepareCall(statement)) {
            s.execute();
        }
    }
}
