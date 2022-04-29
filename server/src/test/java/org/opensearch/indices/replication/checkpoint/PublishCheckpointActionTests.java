/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.copy.ReplicationCheckpoint;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.*;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;

public class PublishCheckpointActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private CapturingTransport transport;
    private ClusterService clusterService;
    private TransportService transportService;
    private ShardStateAction shardStateAction;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        shardStateAction = new ShardStateAction(clusterService, transportService, null, null, threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        try {
            IOUtils.close(transportService, clusterService, transport);
        } finally {
            terminate(threadPool);
        }
        super.tearDown();
    }

    public void testPublishCheckpointActionOnPrimary() throws InterruptedException{
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, clusterService.getClusterSettings());

        final PublishCheckpointAction action = new PublishCheckpointAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet())
        );

        final ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(indexShard.shardId(), 1111, 111, 11, 1);

        final PublishCheckpointRequest request = new PublishCheckpointRequest(checkpoint);

        final CountDownLatch latch = new CountDownLatch(1);
        action.shardOperationOnPrimary(request, indexShard, new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result -> {
            // we should forward the request containing the current retention leases to the replica
            assertThat(result.replicaRequest(), sameInstance(request));
        }), latch));
        latch.await();

    }

    public void testPublishCheckpointActionOnReplica() {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, clusterService.getClusterSettings());


        final PublishCheckpointAction action = new PublishCheckpointAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet())
        );

        final ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(indexShard.shardId(), 1111, 111, 11, 1);

        final PublishCheckpointRequest request = new PublishCheckpointRequest(checkpoint);

        final PlainActionFuture<TransportReplicationAction.ReplicaResult> listener = PlainActionFuture.newFuture();
        action.shardOperationOnReplica(request, indexShard, listener);
        final TransportReplicationAction.ReplicaResult result = listener.actionGet();

        // the result should indicate success
        final AtomicBoolean success = new AtomicBoolean();
        result.runPostReplicaActions(ActionListener.wrap(r -> success.set(true), e -> fail(e.toString())));
        assertTrue(success.get());

    }
}
