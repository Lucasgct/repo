/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.telemetry.tracing.attributes.Attributes;

import java.util.Objects;

/**
 * Context for span details.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class SpanCreationContext {
    private String spanName;
    private Attributes attributes;
    private SpanKind spanKind = SpanKind.INTERNAL;
    private SpanContext parent;

    /**
     * Factory method to create {@link SpanCreationContext}
     * @return spanCreationContext
     */
    public static SpanCreationContext create() {
        return new SpanCreationContext();
    }

    /**
     * Constructor.
     */
    private SpanCreationContext() {}

    /**
     * Sets the span type to server.
     * @return spanCreationContext
     */
    public SpanCreationContext server() {
        this.spanKind = SpanKind.SERVER;
        return this;
    }

    /**
     * Sets the span type to client.
     * @return spanCreationContext
     */
    public SpanCreationContext client() {
        this.spanKind = SpanKind.CLIENT;
        return this;
    }

    /**
     * Sets the span type to internal.
     * @return spanCreationContext
     */
    public SpanCreationContext internal() {
        this.spanKind = SpanKind.INTERNAL;
        return this;
    }

    /**
     * Sets the span name.
     * @param spanName span name.
     * @return spanCreationContext
     */
    public SpanCreationContext name(String spanName) {
        this.spanName = spanName;
        return this;
    }

    /**
     * Sets the span attributes.
     * @param attributes attributes.
     * @return spanCreationContext
     */
    public SpanCreationContext attributes(Attributes attributes) {
        this.attributes = attributes;
        return this;
    }

    /**
     * Sets the parent for spann
     * @param parent parent
     * @return spanCreationContext
     */
    public SpanCreationContext parent(SpanContext parent) {
        this.parent = parent;
        return this;
    }

    /**
     * Returns the span name.
     * @return span name
     */
    public String getSpanName() {
        return spanName;
    }

    /**
     * Returns the span attributes.
     * @return attributes.
     */
    public Attributes getAttributes() {
        return attributes;
    }

    /**
     * Returns the span kind.
     * @return spankind.
     */
    public SpanKind getSpanKind() {
        return spanKind;
    }

    /**
     * Returns the parent span
     * @return parent.
     */
    public SpanContext getParent() {
        return parent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SpanCreationContext)) return false;
        SpanCreationContext that = (SpanCreationContext) o;
        return spanName.equals(that.spanName)
            && attributes.equals(that.attributes)
            && spanKind == that.spanKind
            && parent.equals(that.parent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(spanName, attributes, spanKind, parent);
    }
}
