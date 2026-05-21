/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.kingbase;

import org.apache.commons.lang3.StringUtils;

import io.debezium.function.Predicates;

import java.util.function.Predicate;

/** Applies the include/exclude filters to logical decoding message prefix. */
public class LogicalDecodingMessageFilter {

    private final Predicate<String> filter;

    public LogicalDecodingMessageFilter(String inclusionString, String exclusionString) {
        Predicate<String> inclusions =
                StringUtils.isNotEmpty(inclusionString)
                        ? Predicates.includes(inclusionString)
                        : null;
        Predicate<String> exclusions =
                StringUtils.isNotEmpty(exclusionString)
                        ? Predicates.excludes(exclusionString)
                        : null;
        Predicate<String> filter = inclusions != null ? inclusions : exclusions;
        this.filter = filter != null ? filter : (id) -> true;
    }

    public boolean isIncluded(String prefix) {
        return filter.test(prefix);
    }
}
