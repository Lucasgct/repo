/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

/**
 * Base span
 */
public abstract class AbstractSpan implements Span {

    /**
     * name of the span
     */
    private final String spanName;
    /**
     * span's parent span
     */
    private final Span parentSpan;
    /**
     * span's level
     */
    private final Level level;

    /**
     * Base constructor
     * @param spanName name of the span
     * @param parentSpan span's parent span
     * @param level span's level
     */
    protected AbstractSpan(String spanName, Span parentSpan, Level level) {
        this.spanName = spanName;
        this.parentSpan = parentSpan;
        this.level = level;
    }

    @Override
    public Span getParentSpan() {
        return parentSpan;
    }

    @Override
    public String getSpanName() {
        return spanName;
    }

    @Override
    public Level getLevel() {
        return level;
    }
}
