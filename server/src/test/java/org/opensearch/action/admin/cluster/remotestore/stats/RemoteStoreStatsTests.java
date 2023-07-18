/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.compareStatsResponse;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createStatsForNewReplica;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createShardRouting;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createStatsForNewPrimary;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createStatsForRemoteStoreRestoredPrimary;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

public class RemoteStoreStatsTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private ShardId shardId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("remote_store_stats_test");
        shardId = new ShardId("index", "uuid", 0);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testXContentBuilderWithPrimaryShard() throws IOException {
        RemoteRefreshSegmentTracker.Stats uploadStats = createStatsForNewPrimary(shardId);
        ShardRouting routing = createShardRouting(shardId, true);
        RemoteStoreStats stats = new RemoteStoreStats(uploadStats, routing);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        stats.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        compareStatsResponse(jsonObject, uploadStats, routing);
    }

    public void testXContentBuilderWithReplicaShard() throws IOException {
        RemoteRefreshSegmentTracker.Stats downloadStats = createStatsForNewReplica(shardId);
        ShardRouting routing = createShardRouting(shardId, false);
        RemoteStoreStats stats = new RemoteStoreStats(downloadStats, routing);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        stats.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        compareStatsResponse(jsonObject, downloadStats, routing);
    }

    public void testXContentBuilderWithRemoteStoreRestoredShard() throws IOException {
        RemoteRefreshSegmentTracker.Stats remotestoreRestoredShardStats = createStatsForRemoteStoreRestoredPrimary(shardId);
        ShardRouting routing = createShardRouting(shardId, true);
        RemoteStoreStats stats = new RemoteStoreStats(remotestoreRestoredShardStats, routing);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        stats.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        compareStatsResponse(jsonObject, remotestoreRestoredShardStats, routing);
    }

    public void testSerializationForPrimaryShard() throws Exception {
        RemoteRefreshSegmentTracker.Stats primaryShardStats = createStatsForNewPrimary(shardId);
        RemoteStoreStats stats = new RemoteStoreStats(primaryShardStats, createShardRouting(shardId, true));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteRefreshSegmentTracker.Stats deserializedStats = new RemoteStoreStats(in).getStats();
                assertEquals(stats.getStats().refreshTimeLagMs, deserializedStats.refreshTimeLagMs);
                assertEquals(stats.getStats().localRefreshNumber, deserializedStats.localRefreshNumber);
                assertEquals(stats.getStats().remoteRefreshNumber, deserializedStats.remoteRefreshNumber);
                assertEquals(stats.getStats().uploadBytesStarted, deserializedStats.uploadBytesStarted);
                assertEquals(stats.getStats().uploadBytesSucceeded, deserializedStats.uploadBytesSucceeded);
                assertEquals(stats.getStats().uploadBytesFailed, deserializedStats.uploadBytesFailed);
                assertEquals(stats.getStats().totalUploadsStarted, deserializedStats.totalUploadsStarted);
                assertEquals(stats.getStats().totalUploadsFailed, deserializedStats.totalUploadsFailed);
                assertEquals(stats.getStats().totalUploadsSucceeded, deserializedStats.totalUploadsSucceeded);
                assertEquals(stats.getStats().rejectionCount, deserializedStats.rejectionCount);
                assertEquals(stats.getStats().consecutiveFailuresCount, deserializedStats.consecutiveFailuresCount);
                assertEquals(stats.getStats().uploadBytesMovingAverage, deserializedStats.uploadBytesMovingAverage, 0);
                assertEquals(stats.getStats().uploadBytesPerSecMovingAverage, deserializedStats.uploadBytesPerSecMovingAverage, 0);
                assertEquals(stats.getStats().uploadTimeMovingAverage, deserializedStats.uploadTimeMovingAverage, 0);
                assertEquals(stats.getStats().bytesLag, deserializedStats.bytesLag);
                assertEquals(0, deserializedStats.totalDownloadsStarted);
                assertEquals(0, deserializedStats.totalDownloadsSucceeded);
                assertEquals(0, deserializedStats.totalDownloadsFailed);
                assertEquals(0, deserializedStats.downloadBytesStarted);
                assertEquals(0, deserializedStats.downloadBytesFailed);
                assertEquals(0, deserializedStats.downloadBytesSucceeded);
                assertEquals(0, deserializedStats.lastSuccessfulSegmentDownloadBytes);
                assertEquals(0, deserializedStats.lastDownloadTimestampMs);
            }
        }
    }

    public void testSerializationForReplicaShard() throws Exception {
        RemoteRefreshSegmentTracker.Stats replicaShardStats = createStatsForNewReplica(shardId);
        RemoteStoreStats stats = new RemoteStoreStats(replicaShardStats, createShardRouting(shardId, false));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteRefreshSegmentTracker.Stats deserializedStats = new RemoteStoreStats(in).getStats();
                assertEquals(0, deserializedStats.refreshTimeLagMs);
                assertEquals(0, deserializedStats.localRefreshNumber);
                assertEquals(0, deserializedStats.remoteRefreshNumber);
                assertEquals(0, deserializedStats.uploadBytesStarted);
                assertEquals(0, deserializedStats.uploadBytesSucceeded);
                assertEquals(0, deserializedStats.uploadBytesFailed);
                assertEquals(0, deserializedStats.totalUploadsStarted);
                assertEquals(0, deserializedStats.totalUploadsFailed);
                assertEquals(0, deserializedStats.totalUploadsSucceeded);
                assertEquals(0, deserializedStats.rejectionCount);
                assertEquals(0, deserializedStats.consecutiveFailuresCount);
                assertEquals(0, deserializedStats.bytesLag);
                assertEquals(stats.getStats().totalDownloadsStarted, deserializedStats.totalDownloadsStarted);
                assertEquals(stats.getStats().totalDownloadsSucceeded, deserializedStats.totalDownloadsSucceeded);
                assertEquals(stats.getStats().totalDownloadsFailed, deserializedStats.totalDownloadsFailed);
                assertEquals(stats.getStats().downloadBytesStarted, deserializedStats.downloadBytesStarted);
                assertEquals(stats.getStats().downloadBytesFailed, deserializedStats.downloadBytesFailed);
                assertEquals(stats.getStats().downloadBytesSucceeded, deserializedStats.downloadBytesSucceeded);
                assertEquals(stats.getStats().lastSuccessfulSegmentDownloadBytes, deserializedStats.lastSuccessfulSegmentDownloadBytes);
                assertEquals(stats.getStats().lastDownloadTimestampMs, deserializedStats.lastDownloadTimestampMs);
                assertEquals(stats.getStats().downloadBytesPerSecMovingAverage, deserializedStats.downloadBytesPerSecMovingAverage, 0);
                assertEquals(stats.getStats().downloadTimeMovingAverage, deserializedStats.downloadTimeMovingAverage, 0);
                assertEquals(stats.getStats().downloadBytesMovingAverage, deserializedStats.downloadBytesMovingAverage, 0);
            }
        }
    }

    public void testSerializationForRemoteStoreRestoredPrimaryShard() throws Exception {
        RemoteRefreshSegmentTracker.Stats primaryShardStats = createStatsForRemoteStoreRestoredPrimary(shardId);
        RemoteStoreStats stats = new RemoteStoreStats(primaryShardStats, createShardRouting(shardId, true));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteRefreshSegmentTracker.Stats deserializedStats = new RemoteStoreStats(in).getStats();
                assertEquals(stats.getStats().refreshTimeLagMs, deserializedStats.refreshTimeLagMs);
                assertEquals(stats.getStats().localRefreshNumber, deserializedStats.localRefreshNumber);
                assertEquals(stats.getStats().remoteRefreshNumber, deserializedStats.remoteRefreshNumber);
                assertEquals(stats.getStats().uploadBytesStarted, deserializedStats.uploadBytesStarted);
                assertEquals(stats.getStats().uploadBytesSucceeded, deserializedStats.uploadBytesSucceeded);
                assertEquals(stats.getStats().uploadBytesFailed, deserializedStats.uploadBytesFailed);
                assertEquals(stats.getStats().totalUploadsStarted, deserializedStats.totalUploadsStarted);
                assertEquals(stats.getStats().totalUploadsFailed, deserializedStats.totalUploadsFailed);
                assertEquals(stats.getStats().totalUploadsSucceeded, deserializedStats.totalUploadsSucceeded);
                assertEquals(stats.getStats().rejectionCount, deserializedStats.rejectionCount);
                assertEquals(stats.getStats().consecutiveFailuresCount, deserializedStats.consecutiveFailuresCount);
                assertEquals(stats.getStats().uploadBytesMovingAverage, deserializedStats.uploadBytesMovingAverage, 0);
                assertEquals(stats.getStats().uploadBytesPerSecMovingAverage, deserializedStats.uploadBytesPerSecMovingAverage, 0);
                assertEquals(stats.getStats().uploadTimeMovingAverage, deserializedStats.uploadTimeMovingAverage, 0);
                assertEquals(stats.getStats().bytesLag, deserializedStats.bytesLag);
                assertEquals(stats.getStats().totalDownloadsStarted, deserializedStats.totalDownloadsStarted);
                assertEquals(stats.getStats().totalDownloadsSucceeded, deserializedStats.totalDownloadsSucceeded);
                assertEquals(stats.getStats().totalDownloadsFailed, deserializedStats.totalDownloadsFailed);
                assertEquals(stats.getStats().downloadBytesStarted, deserializedStats.downloadBytesStarted);
                assertEquals(stats.getStats().downloadBytesFailed, deserializedStats.downloadBytesFailed);
                assertEquals(stats.getStats().downloadBytesSucceeded, deserializedStats.downloadBytesSucceeded);
                assertEquals(stats.getStats().lastSuccessfulSegmentDownloadBytes, deserializedStats.lastSuccessfulSegmentDownloadBytes);
                assertEquals(stats.getStats().lastDownloadTimestampMs, deserializedStats.lastDownloadTimestampMs);
                assertEquals(stats.getStats().downloadBytesPerSecMovingAverage, deserializedStats.downloadBytesPerSecMovingAverage, 0);
                assertEquals(stats.getStats().downloadTimeMovingAverage, deserializedStats.downloadTimeMovingAverage, 0);
                assertEquals(stats.getStats().downloadBytesMovingAverage, deserializedStats.downloadBytesMovingAverage, 0);
            }
        }
    }
}
