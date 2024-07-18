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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.admin.cluster.settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterManagerTaskKeys;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.remote.RemoteMigrationIndexMetadataUpdater;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.index.remote.RemoteMigrationIndexMetadataUpdater.indexHasRemoteStoreSettings;

/**
 * Transport action for updating cluster settings
 *
 * @opensearch.internal
 */
public class TransportClusterUpdateSettingsAction extends TransportClusterManagerNodeAction<
    ClusterUpdateSettingsRequest,
    ClusterUpdateSettingsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterUpdateSettingsAction.class);

    private final AllocationService allocationService;

    private final ClusterSettings clusterSettings;

    private final ClusterManagerTaskThrottler.ThrottlingKey clusterUpdateSettingTaskKey;

    @Inject
    public TransportClusterUpdateSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        AllocationService allocationService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterSettings clusterSettings
    ) {
        super(
            ClusterUpdateSettingsAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterUpdateSettingsRequest::new,
            indexNameExpressionResolver
        );
        this.allocationService = allocationService;
        this.clusterSettings = clusterSettings;

        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        clusterUpdateSettingTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTaskKeys.CLUSTER_UPDATE_SETTINGS_KEY, true);

    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterUpdateSettingsRequest request, ClusterState state) {
        // allow for dedicated changes to the metadata blocks, so we don't block those to allow to "re-enable" it
        if (request.transientSettings().size() + request.persistentSettings().size() == 1) {
            // only one setting
            if (Metadata.SETTING_READ_ONLY_SETTING.exists(request.persistentSettings())
                || Metadata.SETTING_READ_ONLY_SETTING.exists(request.transientSettings())
                || Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.exists(request.transientSettings())
                || Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.exists(request.persistentSettings())) {
                // one of the settings above as the only setting in the request means - resetting the block!
                return null;
            }
        }
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterUpdateSettingsResponse read(StreamInput in) throws IOException {
        return new ClusterUpdateSettingsResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        final ClusterUpdateSettingsRequest request,
        final ClusterState state,
        final ActionListener<ClusterUpdateSettingsResponse> listener
    ) {
        final SettingsUpdater updater = new SettingsUpdater(clusterSettings);
        clusterService.submitStateUpdateTask(
            "cluster_update_settings",
            new AckedClusterStateUpdateTask<ClusterUpdateSettingsResponse>(Priority.IMMEDIATE, request, listener) {

                private volatile boolean changed = false;

                private volatile boolean isSwitchingToStrictMode = false;

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return clusterUpdateSettingTaskKey;
                }

                @Override
                protected ClusterUpdateSettingsResponse newResponse(boolean acknowledged) {
                    return new ClusterUpdateSettingsResponse(acknowledged, updater.getTransientUpdates(), updater.getPersistentUpdate());
                }

                @Override
                public void onAllNodesAcked(@Nullable Exception e) {
                    if (changed) {
                        reroute(true);
                    } else {
                        super.onAllNodesAcked(e);
                    }
                }

                @Override
                public void onAckTimeout() {
                    if (changed) {
                        reroute(false);
                    } else {
                        super.onAckTimeout();
                    }
                }

                private void reroute(final boolean updateSettingsAcked) {
                    // We're about to send a second update task, so we need to check if we're still the elected cluster-manager
                    // For example the minimum_master_node could have been breached and we're no longer elected cluster-manager,
                    // so we should *not* execute the reroute.
                    if (!clusterService.state().nodes().isLocalNodeElectedClusterManager()) {
                        logger.debug("Skipping reroute after cluster update settings, because node is no longer cluster-manager");
                        listener.onResponse(
                            new ClusterUpdateSettingsResponse(
                                updateSettingsAcked,
                                updater.getTransientUpdates(),
                                updater.getPersistentUpdate()
                            )
                        );
                        return;
                    }

                    // The reason the reroute needs to be send as separate update task, is that all the *cluster* settings are encapsulate
                    // in the components (e.g. FilterAllocationDecider), so the changes made by the first call aren't visible
                    // to the components until the ClusterStateListener instances have been invoked, but are visible after
                    // the first update task has been completed.
                    clusterService.submitStateUpdateTask(
                        "reroute_after_cluster_update_settings",
                        new AckedClusterStateUpdateTask<ClusterUpdateSettingsResponse>(Priority.URGENT, request, listener) {

                            @Override
                            public boolean mustAck(DiscoveryNode discoveryNode) {
                                // we wait for the reroute ack only if the update settings was acknowledged
                                return updateSettingsAcked;
                            }

                            @Override
                            // we return when the cluster reroute is acked or it times out but the acknowledged flag depends on whether the
                            // update settings was acknowledged
                            protected ClusterUpdateSettingsResponse newResponse(boolean acknowledged) {
                                return new ClusterUpdateSettingsResponse(
                                    updateSettingsAcked && acknowledged,
                                    updater.getTransientUpdates(),
                                    updater.getPersistentUpdate()
                                );
                            }

                            @Override
                            public void onNoLongerClusterManager(String source) {
                                logger.debug(
                                    "failed to preform reroute after cluster settings were updated - current node is no longer a cluster-manager"
                                );
                                listener.onResponse(
                                    new ClusterUpdateSettingsResponse(
                                        updateSettingsAcked,
                                        updater.getTransientUpdates(),
                                        updater.getPersistentUpdate()
                                    )
                                );
                            }

                            @Override
                            public void onFailure(String source, Exception e) {
                                // if the reroute fails we only log
                                logger.debug(() -> new ParameterizedMessage("failed to perform [{}]", source), e);
                                listener.onFailure(new OpenSearchException("reroute after update settings failed", e));
                            }

                            @Override
                            public ClusterState execute(final ClusterState currentState) {
                                // now, reroute in case things that require it changed (e.g. number of replicas)
                                return allocationService.reroute(currentState, "reroute after cluster update settings");
                            }
                        }
                    );
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.debug(() -> new ParameterizedMessage("failed to perform [{}]", source), e);
                    super.onFailure(source, e);
                }

                @Override
                public ClusterState execute(final ClusterState currentState) {
                    boolean isCompatibilityModeChanging = validateCompatibilityModeSettingRequest(request, state);
                    final ClusterState clusterState = updater.updateSettings(
                        currentState,
                        clusterSettings.upgradeSettings(request.transientSettings()),
                        clusterSettings.upgradeSettings(request.persistentSettings()),
                        logger
                    );
                    /*
                     Remote Store migration: Checks if the applied cluster settings
                     has switched the cluster to STRICT mode. If so, checks and applies
                     appropriate index settings depending on the current set of node types
                     in the cluster

                     This has been intentionally done after the cluster settings update
                     flow. That way we are not interfering with the usual settings update
                     and the cluster state mutation that comes along with it
                    */
                    if (isCompatibilityModeChanging && isSwitchToStrictCompatibilityMode(request)) {
                        ClusterState newStateAfterIndexMdChanges = finalizeMigration(clusterState);
                        changed = newStateAfterIndexMdChanges != currentState;
                        return newStateAfterIndexMdChanges;
                    }
                    changed = clusterState != currentState;
                    return clusterState;
                }
            }
        );
    }

    /**
     * Runs various checks associated with changing cluster compatibility mode
     *
     * @param request cluster settings update request, for settings to be updated and new values
     * @param clusterState current state of cluster, for information on nodes
     * @return true if the incoming cluster settings update request is switching compatibility modes
     */
    public boolean validateCompatibilityModeSettingRequest(ClusterUpdateSettingsRequest request, ClusterState clusterState) {
        Settings settings = Settings.builder().put(request.persistentSettings()).put(request.transientSettings()).build();
        if (RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING.exists(settings)) {
            String value = RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING.get(settings).mode;
            validateAllNodesOfSameVersion(clusterState.nodes());
            if (RemoteStoreNodeService.CompatibilityMode.STRICT.mode.equals(value)) {
                validateAllNodesOfSameType(clusterState.nodes());
            }
            return true;
        }
        return false;
    }

    /**
     * Verifies that while trying to change the compatibility mode, all nodes must have the same version.
     * If not, it throws SettingsException error
     * @param discoveryNodes current discovery nodes in the cluster
     */
    private void validateAllNodesOfSameVersion(DiscoveryNodes discoveryNodes) {
        if (discoveryNodes.getMaxNodeVersion().equals(discoveryNodes.getMinNodeVersion()) == false) {
            throw new SettingsException("can not change the compatibility mode when all the nodes in cluster are not of the same version");
        }
    }

    /**
     * Verifies that while trying to switch to STRICT compatibility mode, all nodes must be of the
     * same type (all remote or all non-remote). If not, it throws SettingsException error
     * @param discoveryNodes current discovery nodes in the cluster
     */
    private void validateAllNodesOfSameType(DiscoveryNodes discoveryNodes) {
        Set<Boolean> nodeTypes = discoveryNodes.getNodes()
            .values()
            .stream()
            .map(DiscoveryNode::isRemoteStoreNode)
            .collect(Collectors.toSet());
        if (nodeTypes.size() != 1) {
            throw new SettingsException(
                "can not switch to STRICT compatibility mode when the cluster contains both remote and non-remote nodes"
            );
        }
    }

    /**
     * Finalizes the docrep to remote-store migration process by applying remote store based index settings
     * on indices that are missing them. No-Op if all indices already have the settings applied through
     * IndexMetadataUpdater
     *
     * @param incomingState mutated cluster state after cluster settings were applied
     * @return new cluster state with index settings updated
     */
    public ClusterState finalizeMigration(ClusterState incomingState) {
        Map<String, DiscoveryNode> discoveryNodeMap = incomingState.nodes().getNodes();
        if (discoveryNodeMap.isEmpty() == false) {
            // At this point, we have already validated that all nodes in the cluster are of uniform type.
            // Either all of them are remote store enabled, or all of them are docrep enabled
            boolean remoteStoreEnabledNodePresent = discoveryNodeMap.values().stream().findFirst().get().isRemoteStoreNode();
            if (remoteStoreEnabledNodePresent == true) {
                List<IndexMetadata> indicesWithoutRemoteStoreSettings = getIndicesWithoutRemoteStoreSettings(incomingState);
                if (indicesWithoutRemoteStoreSettings.isEmpty() == true) {
                    logger.info("All indices in the cluster has remote store based index settings");
                } else {
                    Metadata mutatedMetadata = applyRemoteStoreSettings(incomingState, indicesWithoutRemoteStoreSettings);
                    return ClusterState.builder(incomingState).metadata(mutatedMetadata).build();
                }
            } else {
                logger.debug("All nodes in the cluster are not remote nodes. Skipping.");
            }
        }
        return incomingState;
    }

    /**
     * Filters out indices which does not have remote store based
     * index settings applied even after all shard copies have
     * migrated to remote store enabled nodes
     */
    private List<IndexMetadata> getIndicesWithoutRemoteStoreSettings(ClusterState clusterState) {
        Collection<IndexMetadata> allIndicesMetadata = clusterState.metadata().indices().values();
        if (allIndicesMetadata.isEmpty() == false) {
            List<IndexMetadata> indicesWithoutRemoteSettings = allIndicesMetadata.stream()
                .filter(idxMd -> indexHasRemoteStoreSettings(idxMd.getSettings()) == false)
                .collect(Collectors.toList());
            logger.debug(
                "Attempting to switch to strict mode. Count of indices without remote store settings {}",
                indicesWithoutRemoteSettings.size()
            );
            return indicesWithoutRemoteSettings;
        }
        return Collections.emptyList();
    }

    /**
     * Applies remote store index settings through {@link RemoteMigrationIndexMetadataUpdater}
     */
    private Metadata applyRemoteStoreSettings(ClusterState clusterState, List<IndexMetadata> indicesWithRemoteStoreSettings) {
        Metadata.Builder metadataBuilder = Metadata.builder(clusterState.getMetadata());
        RoutingTable currentRoutingTable = clusterState.getRoutingTable();
        DiscoveryNodes currentDiscoveryNodes = clusterState.getNodes();
        Settings currentClusterSettings = clusterState.metadata().settings();
        for (IndexMetadata indexMetadata : indicesWithRemoteStoreSettings) {
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
            RemoteMigrationIndexMetadataUpdater indexMetadataUpdater = new RemoteMigrationIndexMetadataUpdater(
                currentDiscoveryNodes,
                currentRoutingTable,
                indexMetadata,
                currentClusterSettings,
                logger
            );
            indexMetadataUpdater.maybeAddRemoteIndexSettings(indexMetadataBuilder, indexMetadata.getIndex().getName());
            metadataBuilder.put(indexMetadataBuilder);
        }
        return metadataBuilder.build();
    }

    /**
     * Checks if the incoming cluster settings payload is attempting to switch
     * the cluster to `STRICT` compatibility mode
     * Visible only for tests
     */
    public boolean isSwitchToStrictCompatibilityMode(ClusterUpdateSettingsRequest request) {
        Settings incomingSettings = Settings.builder().put(request.persistentSettings()).put(request.transientSettings()).build();
        return RemoteStoreNodeService.CompatibilityMode.STRICT.mode.equals(
            RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING.get(incomingSettings).mode
        );
    }
}
