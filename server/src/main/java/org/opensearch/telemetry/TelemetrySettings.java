/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.List;
import java.util.function.Function;

/**
 * Wrapper class to encapsulate tracing related settings
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TelemetrySettings {
    public static final Setting<Boolean> TRACER_ENABLED_SETTING = Setting.boolSetting(
        "telemetry.tracer.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> TRACER_FEATURE_ENABLED_SETTING = Setting.boolSetting(
        "telemetry.feature.tracer.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    public static final Setting<Boolean> METRICS_FEATURE_ENABLED_SETTING = Setting.boolSetting(
        "telemetry.feature.metrics.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /**
     * Probability of sampler
     */
    public static final Setting<Double> TRACER_SAMPLER_PROBABILITY = Setting.doubleSetting(
        "telemetry.tracer.sampler.probability",
        0.01d,
        0.00d,
        1.00d,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * metrics publish interval in seconds.
     */
    public static final Setting<TimeValue> METRICS_PUBLISH_INTERVAL_SETTING = Setting.timeSetting(
        "telemetry.otel.metrics.publish.interval",
        TimeValue.timeValueSeconds(60),
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /**
     * Probability of action based sampler
     */
    public static final Setting<Double> TRACER_SAMPLER_ACTION_PROBABILITY = Setting.doubleSetting(
        "telemetry.tracer.action.sampler.probability",
        0.001d,
        0.000d,
        1.00d,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Order of execution of samplers
     */
    public static final Setting<List<String>> TRACER_SPAN_SAMPLER_CLASSES = Setting.listSetting(
        "telemetry.otel.tracer.span.sampler.classes",
        List.of(
            "org.opensearch.telemetry.tracing.sampler.ProbabilisticSampler",
            "org.opensearch.telemetry.tracing.sampler.ProbabilisticTransportActionSampler"
        ),
        Function.identity(),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile boolean tracingEnabled;
    private volatile double samplingProbability;
    private volatile double actionSamplingProbability;
    private final boolean tracingFeatureEnabled;
    private final boolean metricsFeatureEnabled;
    private volatile List<String> samplersOrder;

    public TelemetrySettings(Settings settings, ClusterSettings clusterSettings) {
        this.tracingEnabled = TRACER_ENABLED_SETTING.get(settings);
        this.samplingProbability = TRACER_SAMPLER_PROBABILITY.get(settings);
        this.tracingFeatureEnabled = TRACER_FEATURE_ENABLED_SETTING.get(settings);
        this.metricsFeatureEnabled = METRICS_FEATURE_ENABLED_SETTING.get(settings);
        this.actionSamplingProbability = TRACER_SAMPLER_ACTION_PROBABILITY.get(settings);
        this.samplersOrder = TRACER_SPAN_SAMPLER_CLASSES.get(settings);

        clusterSettings.addSettingsUpdateConsumer(TRACER_ENABLED_SETTING, this::setTracingEnabled);
        clusterSettings.addSettingsUpdateConsumer(TRACER_SAMPLER_PROBABILITY, this::setSamplingProbability);
        clusterSettings.addSettingsUpdateConsumer(TRACER_SAMPLER_ACTION_PROBABILITY, this::setActionSamplingProbability);
        clusterSettings.addSettingsUpdateConsumer(TRACER_SPAN_SAMPLER_CLASSES, this::setSamplingOrder);
    }

    public void setSamplingOrder(List<String> order) {
        this.samplersOrder = order;
    }

    public List<String> getSamplingOrder() {
        return samplersOrder;
    }

    public void setTracingEnabled(boolean tracingEnabled) {
        this.tracingEnabled = tracingEnabled;
    }

    public boolean isTracingEnabled() {
        return tracingEnabled;
    }

    /**
     * Set sampling ratio
     * @param samplingProbability double
     */
    public void setSamplingProbability(double samplingProbability) {
        this.samplingProbability = samplingProbability;
    }

    /**
     * Set sampling ratio for all action
     * @param actionSamplingProbability double
     */
    public void setActionSamplingProbability(double actionSamplingProbability) {
        this.actionSamplingProbability = actionSamplingProbability;
    }

    /**
     * Get sampling ratio
     * @return double
     */
    public double getSamplingProbability() {
        return samplingProbability;
    }

    public boolean isTracingFeatureEnabled() {
        return tracingFeatureEnabled;
    }

    public boolean isMetricsFeatureEnabled() {
        return metricsFeatureEnabled;
    }

    /**
     * Get action sampling ratio
     * @return double value of sampling probability for actions
     */
    public double getActionSamplingProbability() {
        return this.actionSamplingProbability;
    }
}
