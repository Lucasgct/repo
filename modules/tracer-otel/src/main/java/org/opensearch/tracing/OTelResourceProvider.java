/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.opensearch.tracing.exporter.FileSpanExporter;

import java.util.concurrent.TimeUnit;

/**
 * This class encapsulates all OpenTelemetry related resources
 */
final class OTelResourceProvider {

    private static final ContextPropagators contextPropagators;
    private static volatile OpenTelemetry OPEN_TELEMETRY;

    static {
        contextPropagators = ContextPropagators.create(W3CTraceContextPropagator.getInstance());
    }

    static OpenTelemetry getOrCreateOpenTelemetryInstance(TracerSettings tracerSettings) {
        if (OPEN_TELEMETRY == null) {
            synchronized (OTelResourceProvider.class) {
                if (OPEN_TELEMETRY == null) {
                    Resource resource = Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "OpenSearch"));
                    SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                        .addSpanProcessor(
                            BatchSpanProcessor.builder(new FileSpanExporter())
                                .setScheduleDelay(tracerSettings.getExporterDelay().getSeconds(), TimeUnit.SECONDS)
                                .setMaxExportBatchSize(tracerSettings.getExporterBatchSize())
                                .setMaxQueueSize(tracerSettings.getExporterMaxQueueSize())
                                .build()
                        )
                        .setResource(resource)
                        .build();
                    OPEN_TELEMETRY = OpenTelemetrySdk.builder()
                        .setTracerProvider(sdkTracerProvider)
                        .setPropagators(contextPropagators)
                        .buildAndRegisterGlobal();
                }
            }
        }
        return OPEN_TELEMETRY;
    }

    static ContextPropagators getContextPropagators() {
        return contextPropagators;
    }
}
