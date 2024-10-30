/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.grpc;

import org.opensearch.common.Booleans;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.opensearch.common.settings.Setting.boolSetting;
import static org.opensearch.common.settings.Setting.intSetting;
import static org.opensearch.common.settings.Setting.listSetting;

/**
 * Transport settings for http connections
 *
 * @opensearch.internal
 */
public final class GrpcTransportSettings {

    public static final Setting<PortsRange> SETTING_GRPC_PORT = new Setting<>(
        "grpc.port",
        "9200-9300",
        PortsRange::new,
        Property.NodeScope
    );

    public static final Setting<List<String>> SETTING_GRPC_BIND_HOST = listSetting(
        "grpc.bind_host",
        List.of("0.0.0.0"),
        Function.identity(),
        Property.NodeScope
    );

    private GrpcTransportSettings() {}
}
