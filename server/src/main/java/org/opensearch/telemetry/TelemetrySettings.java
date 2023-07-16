/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.listeners.TraceEventListenerService;

/**
 * Wrapper class to encapsulate tracing related settings
 */
public class TelemetrySettings {
    public static final Setting<Boolean> TRACER_ENABLED_SETTING = Setting.boolSetting(
        "telemetry.tracer.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> DIAGNOSIS_ENABLED_SETTING = Setting.boolSetting(
        "telemetry.diagnosis.enabled",
        true, // TODO - change back to false after dev testing
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile boolean tracingEnabled;
    private volatile boolean diagnosisEnabled;

    public TelemetrySettings(Settings settings, ClusterSettings clusterSettings) {
        this.tracingEnabled = TRACER_ENABLED_SETTING.get(settings);

        clusterSettings.addSettingsUpdateConsumer(TRACER_ENABLED_SETTING, this::setTracingEnabled);
        clusterSettings.addSettingsUpdateConsumer(DIAGNOSIS_ENABLED_SETTING, this::setDiagnosisEnabled);
    }

    public void setTracingEnabled(boolean tracingEnabled) {
        this.tracingEnabled = tracingEnabled;
    }

    public void setDiagnosisEnabled(boolean diagnosisEnabled) {
        this.diagnosisEnabled = diagnosisEnabled;
    }

    public boolean isTracingEnabled() {
        return tracingEnabled;
    }

    public boolean isDiagnosisEnabled() {
        return diagnosisEnabled;
    }
}
