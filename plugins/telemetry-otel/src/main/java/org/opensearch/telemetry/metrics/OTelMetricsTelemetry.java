/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.opensearch.common.concurrent.RefCountedReleasable;
import org.opensearch.telemetry.OTelTelemetryPlugin;

import java.io.Closeable;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;

import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;

/**
 * OTel implementation for {@link MetricsTelemetry}
 */
public class OTelMetricsTelemetry<T extends MeterProvider & Closeable> implements MetricsTelemetry {
    private final Meter otelMeter;
    private final RefCountedReleasable<T> refCountedMeterProvider;

    /**
     * Creates OTel based {@link MetricsTelemetry}.
     * @param meterProvider {@link MeterProvider} instance
     */
    public OTelMetricsTelemetry(RefCountedReleasable<T> meterProvider) {
        this.refCountedMeterProvider = meterProvider;
        this.otelMeter = meterProvider.get().get(OTelTelemetryPlugin.INSTRUMENTATION_SCOPE_NAME);
    }

    @Override
    public Counter createCounter(String name, String description, String unit) {
        DoubleCounter doubleCounter = AccessController.doPrivileged(
            (PrivilegedAction<DoubleCounter>) () -> otelMeter.counterBuilder(name)
                .setUnit(unit)
                .setDescription(description)
                .ofDoubles()
                .build()
        );
        return new OTelCounter(doubleCounter);
    }

    @Override
    public Counter createUpDownCounter(String name, String description, String unit) {
        DoubleUpDownCounter doubleUpDownCounter = AccessController.doPrivileged(
            (PrivilegedAction<DoubleUpDownCounter>) () -> otelMeter.upDownCounterBuilder(name)
                .setUnit(unit)
                .setDescription(description)
                .ofDoubles()
                .build()
        );
        return new OTelUpDownCounter(doubleUpDownCounter);
    }

    @Override
    public void close() throws IOException {
        if (refCountedMeterProvider.tryIncRef()) {
            refCountedMeterProvider.close();
        }
    }
}
