/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbase;

import com.kingbase8.util.KSQLException;
import io.debezium.annotation.Immutable;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Error handler for Postgres.
 *
 * @author Gunnar Morling
 */
public class KingbaseErrorHandler extends ErrorHandler {

    @Immutable private static final Set<String> RETRIABLE_EXCEPTION_MESSSAGES;

    public KingbaseErrorHandler(String logicalName, ChangeEventQueue<?> queue) {
        super(KingbaseConnector.class, logicalName, queue);
    }

    static {
        Set<String> tmp = new HashSet<>();
        tmp.add("Database connection failed when writing to copy");
        tmp.add("Database connection failed when reading from copy");
        tmp.add("FATAL: terminating connection due to administrator command");
        tmp.add("An I/O error occurred while sending to the backend");

        RETRIABLE_EXCEPTION_MESSSAGES = Collections.unmodifiableSet(tmp);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable instanceof KSQLException && throwable.getMessage() != null) {
            for (String messageText : RETRIABLE_EXCEPTION_MESSSAGES) {
                if (throwable.getMessage().contains(messageText)) {
                    return true;
                }
            }
        }
        return false;
    }
}
