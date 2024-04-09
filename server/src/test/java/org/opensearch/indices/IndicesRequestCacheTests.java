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

package org.opensearch.indices;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.module.CacheModule;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.AbstractBytesReference;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.cache.request.ShardRequestCache;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.node.Node;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.indices.IndicesRequestCache.INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesRequestCacheTests extends OpenSearchSingleNodeTestCase {
    private ThreadPool threadPool;
    private IndexWriter writer;
    private Directory dir;
    private IndicesRequestCache cache;
    private IndexShard indexShard;

    private ThreadPool getThreadPool() {
        return new ThreadPool(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "default tracer tests").build());
    }

    @Before
    public void setup() throws IOException {
        dir = newDirectory();
        writer = new IndexWriter(dir, newIndexWriterConfig());
        indexShard = createIndex("test").getShard(0);
    }

    @After
    public void cleanup() throws IOException {
        IOUtils.close(writer, dir, cache);
        terminate(threadPool);
    }

    public void testBasicOperationsCache() throws Exception {
        threadPool = getThreadPool();
        cache = getIndicesRequestCache(Settings.EMPTY);
        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = getReader(writer, indexShard.shardId());

        // initial cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, getTermBytes());
        assertEquals("foo", value.streamInput().readString());
        ShardRequestCache requestCacheStats = indexShard.requestCache();
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());

        // cache hit
        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, getTermBytes());
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = indexShard.requestCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // Closing the cache doesn't modify an already returned CacheEntity
        if (randomBoolean()) {
            reader.close();
        } else {
            indexShard.close("test", true, true); // closed shard but reader is still open
            cache.clear(entity);
        }
        cache.cacheCleanupManager.cleanCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(reader);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testBasicOperationsCacheWithFeatureFlag() throws Exception {
        threadPool = getThreadPool();
        Settings settings = Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.PLUGGABLE_CACHE, "true").build();
        cache = getIndicesRequestCache(settings);
        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = getReader(writer, indexShard.shardId());

        // initial cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, getTermBytes());
        assertEquals("foo", value.streamInput().readString());
        ShardRequestCache requestCacheStats = indexShard.requestCache();
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());

        // cache hit
        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, getTermBytes());
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = indexShard.requestCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // Closing the cache doesn't modify an already returned CacheEntity
        if (randomBoolean()) {
            reader.close();
        } else {
            indexShard.close("test", true, true); // closed shard but reader is still open
            cache.clear(entity);
        }
        cache.cacheCleanupManager.cleanCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(reader);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testCacheDifferentReaders() throws Exception {
        threadPool = getThreadPool();
        cache = getIndicesRequestCache(Settings.EMPTY);
        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = getReader(writer, indexShard.shardId());

        if (randomBoolean()) {
            writer.flush();
            IOUtils.close(writer);
            writer = new IndexWriter(dir, newIndexWriterConfig());
        }
        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = getReader(writer, indexShard.shardId());

        // initial cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, getTermBytes());
        ShardRequestCache requestCacheStats = entity.stats();
        assertEquals("foo", value.streamInput().readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        final int cacheSize = requestCacheStats.stats().getMemorySize().bytesAsInt();
        assertEquals(1, cache.numRegisteredCloseListeners());

        // cache the second
        IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        value = cache.getOrCompute(entity, loader, secondReader, getTermBytes());
        requestCacheStats = entity.stats();
        assertEquals("bar", value.streamInput().readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(2, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > cacheSize + value.length());
        assertEquals(2, cache.numRegisteredCloseListeners());

        secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        value = cache.getOrCompute(secondEntity, loader, secondReader, getTermBytes());
        requestCacheStats = entity.stats();
        assertEquals("bar", value.streamInput().readString());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(2, cache.count());

        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, getTermBytes());
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = entity.stats();
        assertEquals(2, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(2, cache.count());

        // Closing the cache doesn't change returned entities
        reader.close();
        cache.cacheCleanupManager.cleanCache();
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertEquals(cacheSize, requestCacheStats.stats().getMemorySize().bytesAsInt());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // release
        if (randomBoolean()) {
            secondReader.close();
        } else {
            indexShard.close("test", true, true); // closed shard but reader is still open
            cache.clear(secondEntity);
        }
        cache.cacheCleanupManager.cleanCache();
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(secondReader);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testCacheCleanupThresholdSettingValidator_Valid_Percentage() {
        String s = IndicesRequestCache.validateStalenessSetting("50%");
        assertEquals("50%", s);
    }

    public void testCacheCleanupThresholdSettingValidator_Valid_Double() {
        String s = IndicesRequestCache.validateStalenessSetting("0.5");
        assertEquals("0.5", s);
    }

    public void testCacheCleanupThresholdSettingValidator_Valid_DecimalPercentage() {
        String s = IndicesRequestCache.validateStalenessSetting("0.5%");
        assertEquals("0.5%", s);
    }

    public void testCacheCleanupThresholdSettingValidator_InValid_MB() {
        assertThrows(IllegalArgumentException.class, () -> { IndicesRequestCache.validateStalenessSetting("50mb"); });
    }

    public void testCacheCleanupThresholdSettingValidator_Invalid_Percentage() {
        assertThrows(IllegalArgumentException.class, () -> { IndicesRequestCache.validateStalenessSetting("500%"); });
    }

    // when staleness threshold is zero, stale keys should be cleaned up every time cache cleaner is invoked.
    public void testCacheCleanupBasedOnZeroThreshold() throws Exception {
        threadPool = getThreadPool();
        Settings settings = Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), "0%").build();
        cache = getIndicesRequestCache(settings);
        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = getReader(writer, indexShard.shardId());
        DirectoryReader secondReader = getReader(writer, indexShard.shardId());

        // Get 2 entries into the cache
        cache.getOrCompute(getEntity(indexShard), getLoader(reader), reader, getTermBytes());
        assertEquals(1, cache.count());

        cache.getOrCompute(getEntity(indexShard), getLoader(secondReader), secondReader, getTermBytes());
        assertEquals(2, cache.count());

        // Close the reader, to be enqueued for cleanup
        // 1 out of 2 keys ie 50% are now stale.
        reader.close();
        // cache count should not be affected
        assertEquals(2, cache.count());
        // clean cache with 0% staleness threshold
        cache.cacheCleanupManager.cleanCache();
        // cleanup should remove the stale-key
        assertEquals(1, cache.count());
        IOUtils.close(secondReader);
    }

    // when staleness count is higher than stale threshold, stale keys should be cleaned up.
    public void testCacheCleanupBasedOnStaleThreshold_StalenessHigherThanThreshold() throws Exception {
        threadPool = getThreadPool();
        Settings settings = Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), "0.49").build();
        cache = getIndicesRequestCache(settings);

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = getReader(writer, indexShard.shardId());
        DirectoryReader secondReader = getReader(writer, indexShard.shardId());

        // Get 2 entries into the cache
        cache.getOrCompute(getEntity(indexShard), getLoader(reader), reader, getTermBytes());
        assertEquals(1, cache.count());

        cache.getOrCompute(getEntity(indexShard), getLoader(reader), secondReader, getTermBytes());
        assertEquals(2, cache.count());

        // no stale keys so far
        assertEquals(0, cache.cacheCleanupManager.getStaleKeysCount().get());
        // Close the reader, to be enqueued for cleanup
        reader.close();
        // 1 out of 2 keys ie 50% are now stale.
        assertEquals(1, cache.cacheCleanupManager.getStaleKeysCount().get());
        // cache count should not be affected
        assertEquals(2, cache.count());

        // clean cache with 49% staleness threshold
        cache.cacheCleanupManager.cleanCache();
        // cleanup should have taken effect with 49% threshold
        assertEquals(1, cache.count());
        assertEquals(0, cache.cacheCleanupManager.getStaleKeysCount().get());

        IOUtils.close(secondReader);
    }

    // when staleness count equal to stale threshold, stale keys should be cleaned up.
    public void testCacheCleanupBasedOnStaleThreshold_StalenessEqualToThreshold() throws Exception {
        threadPool = getThreadPool();
        Settings settings = Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), "0.5").build();
        cache = getIndicesRequestCache(settings);
        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = getReader(writer, indexShard.shardId());
        DirectoryReader secondReader = getReader(writer, indexShard.shardId());

        // Get 2 entries into the cache
        cache.getOrCompute(getEntity(indexShard), getLoader(reader), reader, getTermBytes());
        assertEquals(1, cache.count());

        cache.getOrCompute(getEntity(indexShard), getLoader(secondReader), secondReader, getTermBytes());
        assertEquals(2, cache.count());

        // Close the reader, to be enqueued for cleanup
        reader.close();
        // 1 out of 2 keys ie 50% are now stale.
        assertEquals(1, cache.cacheCleanupManager.getStaleKeysCount().get());
        // cache count should not be affected
        assertEquals(2, cache.count());

        // clean cache with 50% staleness threshold
        cache.cacheCleanupManager.cleanCache();
        // cleanup should have taken effect
        assertEquals(0, cache.cacheCleanupManager.getStaleKeysCount().get());
        assertEquals(1, cache.count());

        IOUtils.close(secondReader);
    }

    // when a cache entry that is Stale is evicted for any reason, we have to deduct the count from our staleness count
    public void testStaleCount_OnRemovalNotificationOfStaleKey_DecrementsStaleCount() throws Exception {
        threadPool = getThreadPool();
        Settings settings = Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), "0.51").build();
        cache = getIndicesRequestCache(settings);
        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = getReader(writer, indexShard.shardId());
        DirectoryReader secondReader = getReader(writer, indexShard.shardId());

        // Get 2 entries into the cache from 2 different readers
        cache.getOrCompute(getEntity(indexShard), getLoader(reader), reader, getTermBytes());
        assertEquals(1, cache.count());

        cache.getOrCompute(getEntity(indexShard), getLoader(secondReader), secondReader, getTermBytes());
        assertEquals(2, cache.count());

        // assert no stale keys are accounted so far
        assertEquals(0, cache.cacheCleanupManager.getStaleKeysCount().get());
        // Close the reader, this should create a stale key
        reader.close();
        // 1 out of 2 keys ie 50% are now stale.
        assertEquals(1, cache.cacheCleanupManager.getStaleKeysCount().get());
        // cache count should not be affected
        assertEquals(2, cache.count());

        IndicesRequestCache.Key key = new IndicesRequestCache.Key(indexShard.shardId(), getTermBytes(), getReaderCacheKeyId(reader));

        cache.onRemoval(new RemovalNotification<IndicesRequestCache.Key, BytesReference>(key, getTermBytes(), RemovalReason.EVICTED));
        // eviction of previous stale key from the cache should decrement staleKeysCount in iRC
        assertEquals(0, cache.cacheCleanupManager.getStaleKeysCount().get());

        IOUtils.close(secondReader);
    }

    // when a cache entry that is NOT Stale is evicted for any reason, staleness count should NOT be deducted
    public void testStaleCount_OnRemovalNotificationOfStaleKey_DoesNotDecrementsStaleCount() throws Exception {
        threadPool = getThreadPool();
        Settings settings = Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), "0.51").build();
        cache = getIndicesRequestCache(settings);
        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = getReader(writer, indexShard.shardId());
        DirectoryReader secondReader = getReader(writer, indexShard.shardId());

        // Get 2 entries into the cache
        cache.getOrCompute(getEntity(indexShard), getLoader(reader), reader, getTermBytes());
        assertEquals(1, cache.count());

        cache.getOrCompute(getEntity(indexShard), getLoader(secondReader), secondReader, getTermBytes());
        assertEquals(2, cache.count());

        // Close the reader, to be enqueued for cleanup
        reader.close();
        AtomicInteger staleKeysCount = cache.cacheCleanupManager.getStaleKeysCount();
        // 1 out of 2 keys ie 50% are now stale.
        assertEquals(1, staleKeysCount.get());
        // cache count should not be affected
        assertEquals(2, cache.count());

        // evict entry from second reader (this reader is not closed)
        IndicesRequestCache.Key key = new IndicesRequestCache.Key(indexShard.shardId(), getTermBytes(), getReaderCacheKeyId(secondReader));

        cache.onRemoval(new RemovalNotification<IndicesRequestCache.Key, BytesReference>(key, getTermBytes(), RemovalReason.EVICTED));
        staleKeysCount = cache.cacheCleanupManager.getStaleKeysCount();
        // eviction of NON-stale key from the cache should NOT decrement staleKeysCount in iRC
        assertEquals(1, staleKeysCount.get());

        IOUtils.close(secondReader);
    }

    // when a cache entry that is NOT Stale is evicted WITHOUT its reader closing, we should NOT deduct it from staleness count
    public void testStaleCount_WithoutReaderClosing_DecrementsStaleCount() throws Exception {
        threadPool = getThreadPool();
        Settings settings = Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), "0.51").build();
        cache = getIndicesRequestCache(settings);

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = getReader(writer, indexShard.shardId());
        DirectoryReader secondReader = getReader(writer, indexShard.shardId());

        // Get 2 entries into the cache from 2 different readers
        cache.getOrCompute(getEntity(indexShard), getLoader(reader), reader, getTermBytes());
        assertEquals(1, cache.count());

        cache.getOrCompute(getEntity(indexShard), getLoader(secondReader), secondReader, getTermBytes());
        assertEquals(2, cache.count());

        // no keys are stale
        assertEquals(0, cache.cacheCleanupManager.getStaleKeysCount().get());
        // create notification for removal of non-stale entry
        IndicesRequestCache.Key key = new IndicesRequestCache.Key(indexShard.shardId(), getTermBytes(), getReaderCacheKeyId(reader));
        cache.onRemoval(new RemovalNotification<IndicesRequestCache.Key, BytesReference>(key, getTermBytes(), RemovalReason.EVICTED));
        // stale keys count should stay zero
        assertEquals(0, cache.cacheCleanupManager.getStaleKeysCount().get());

        IOUtils.close(reader, secondReader);
    }

    // test staleness count based on removal notifications
    public void testStaleCount_OnRemovalNotifications() throws Exception {
        threadPool = getThreadPool();
        Settings settings = Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), "0.51").build();
        cache = getIndicesRequestCache(settings);

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = getReader(writer, indexShard.shardId());

        // Get 5 entries into the cache
        int totalKeys = 5;
        IndicesService.IndexShardCacheEntity entity = null;
        TermQueryBuilder termQuery = null;
        BytesReference termBytes = null;
        for (int i = 1; i <= totalKeys; i++) {
            termQuery = new TermQueryBuilder("id", "" + i);
            termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
            entity = new IndicesService.IndexShardCacheEntity(indexShard);
            Loader loader = new Loader(reader, 0);
            cache.getOrCompute(entity, loader, reader, termBytes);
            assertEquals(i, cache.count());
        }
        // no keys are stale yet
        assertEquals(0, cache.cacheCleanupManager.getStaleKeysCount().get());
        // closing the reader should make all keys stale
        reader.close();
        assertEquals(totalKeys, cache.cacheCleanupManager.getStaleKeysCount().get());

        String readerCacheKeyId = getReaderCacheKeyId(reader);
        IndicesRequestCache.Key key = new IndicesRequestCache.Key(
            ((IndexShard) entity.getCacheIdentity()).shardId(),
            termBytes,
            readerCacheKeyId
        );

        int staleCount = cache.cacheCleanupManager.getStaleKeysCount().get();
        // Notification for Replaced should not deduct the staleCount
        cache.onRemoval(new RemovalNotification<IndicesRequestCache.Key, BytesReference>(key, termBytes, RemovalReason.REPLACED));
        // stale keys count should stay the same
        assertEquals(staleCount, cache.cacheCleanupManager.getStaleKeysCount().get());

        // Notification for all but Replaced should deduct the staleCount
        RemovalReason[] reasons = { RemovalReason.INVALIDATED, RemovalReason.EVICTED, RemovalReason.EXPLICIT, RemovalReason.CAPACITY };
        for (RemovalReason reason : reasons) {
            cache.onRemoval(new RemovalNotification<IndicesRequestCache.Key, BytesReference>(key, termBytes, reason));
            assertEquals(--staleCount, cache.cacheCleanupManager.getStaleKeysCount().get());
        }
    }

    // when staleness count less than the stale threshold, stale keys should NOT be cleaned up.
    public void testCacheCleanupBasedOnStaleThreshold_StalenessLesserThanThreshold() throws Exception {
        threadPool = getThreadPool();
        Settings settings = Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), "51%").build();
        cache = getIndicesRequestCache(settings);

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = getReader(writer, indexShard.shardId());
        DirectoryReader secondReader = getReader(writer, indexShard.shardId());

        // Get 2 entries into the cache
        cache.getOrCompute(getEntity(indexShard), getLoader(reader), reader, getTermBytes());
        assertEquals(1, cache.count());

        cache.getOrCompute(getEntity(indexShard), getLoader(secondReader), secondReader, getTermBytes());
        assertEquals(2, cache.count());

        // Close the reader, to be enqueued for cleanup
        reader.close();
        // 1 out of 2 keys ie 50% are now stale.
        assertEquals(1, cache.cacheCleanupManager.getStaleKeysCount().get());
        // cache count should not be affected
        assertEquals(2, cache.count());

        // clean cache with 51% staleness threshold
        cache.cacheCleanupManager.cleanCache();
        // cleanup should have been ignored
        assertEquals(1, cache.cacheCleanupManager.getStaleKeysCount().get());
        assertEquals(2, cache.count());

        IOUtils.close(secondReader);
    }

    // test the cleanupKeyToCountMap are set appropriately when both readers are closed
    public void testCleanupKeyToCountMapAreSetAppropriately() throws Exception {
        threadPool = getThreadPool();
        Settings settings = Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), "0.51").build();
        cache = getIndicesRequestCache(settings);

        writer.addDocument(newDoc(0, "foo"));
        ShardId shardId = indexShard.shardId();
        DirectoryReader reader = getReader(writer, shardId);
        DirectoryReader secondReader = getReader(writer, shardId);

        // Get 2 entries into the cache from 2 different readers
        cache.getOrCompute(getEntity(indexShard), getLoader(reader), reader, getTermBytes());
        assertEquals(1, cache.count());
        // test the mappings
        ConcurrentMap<ShardId, HashMap<String, Integer>> cleanupKeyToCountMap = cache.cacheCleanupManager.getCleanupKeyToCountMap();
        assertEquals(1, (int) cleanupKeyToCountMap.get(shardId).get(getReaderCacheKeyId(reader)));

        cache.getOrCompute(getEntity(indexShard), getLoader(secondReader), secondReader, getTermBytes());
        // test the mapping
        assertEquals(2, cache.count());
        assertEquals(1, (int) cleanupKeyToCountMap.get(shardId).get(getReaderCacheKeyId(secondReader)));

        // Close the reader, to create stale entries
        reader.close();
        // 1 out of 2 keys ie 50% are now stale.
        assertEquals(1, cache.cacheCleanupManager.getStaleKeysCount().get());
        // cache count should not be affected
        assertEquals(2, cache.count());

        // test the mapping
        assertFalse(cleanupKeyToCountMap.get(shardId).containsKey(getReaderCacheKeyId(reader)));

        // second reader's mapping should not be affected
        assertEquals(1, (int) cleanupKeyToCountMap.get(shardId).get(getReaderCacheKeyId(secondReader)));

        // Close the second reader
        secondReader.close();
        // both keys should now be stale
        assertEquals(2, cache.cacheCleanupManager.getStaleKeysCount().get());
        // cache count should not be affected
        assertEquals(2, cache.count());

        // test the mapping
        // since all the readers of this shard is closed,
        // the cleanupKeyToCountMap should have no entries
        assertEquals(0, cleanupKeyToCountMap.size());
    }

    // when registering a closed listener raises an exception, we should
    @Test(expected = Exception.class)
    @SuppressForbidden(reason = "only way to avoid registering a reader after caching a key")
    public void testAddReaderCloseListenerRaisingException_shouldTrackStaleCountAppropriately() throws Exception {
        IndexReader.CacheHelper cacheHelper = mock(IndexReader.CacheHelper.class);
        doThrow(new Exception("Mock exception")).when(cacheHelper).addClosedListener(any());
        DirectoryReader reader = getReader(writer, indexShard.shardId());

        // assert there are no other entries
        assertEquals(0, cache.count());
        assertEquals(0, cache.cacheCleanupManager.getStaleKeysCount().get());

        cache.getOrCompute(getEntity(indexShard), getLoader(reader), reader, getTermBytes());
        // the entry should be cached
        assertEquals(1, cache.count());
        // cacheCleanupManager should have incremented the stale keys count
        assertEquals(1, cache.cacheCleanupManager.getStaleKeysCount().get());
        // cacheCleanupManager should have removed the entry from the map
        assertFalse(cache.cacheCleanupManager.getCleanupKeyToCountMap().containsKey(indexShard.shardId()));

        IndicesRequestCache.Key key = new IndicesRequestCache.Key(indexShard.shardId(), getTermBytes(), getReaderCacheKeyId(reader));
        cache.onRemoval(new RemovalNotification<IndicesRequestCache.Key, BytesReference>(key, getTermBytes(), RemovalReason.INVALIDATED));
        // eviction of previous stale key from the cache should decrement staleKeysCount in iRC
        assertEquals(0, cache.cacheCleanupManager.getStaleKeysCount().get());
    }

    @Test(expected = Exception.class)
    @SuppressForbidden(reason = "only way to avoid registering a reader after caching a key")
    public void testAddReaderCloseListenerRaisingException_shouldCleanupCacheCorrectly() throws Exception {
        IndexReader.CacheHelper cacheHelper = mock(IndexReader.CacheHelper.class);
        doThrow(new Exception("Mock exception")).when(cacheHelper).addClosedListener(any());
        DirectoryReader reader = getReader(writer, indexShard.shardId());

        // assert there are no other entries
        assertEquals(0, cache.count());
        assertEquals(0, cache.cacheCleanupManager.getStaleKeysCount().get());

        cache.getOrCompute(getEntity(indexShard), getLoader(reader), reader, getTermBytes());
        // the entry should be cached
        assertEquals(1, cache.count());
        // cacheCleanupManager should have incremented the stale keys count
        assertEquals(1, cache.cacheCleanupManager.getStaleKeysCount().get());
        // cacheCleanupManager should have removed the entry from the map
        assertFalse(cache.cacheCleanupManager.getCleanupKeyToCountMap().containsKey(indexShard.shardId()));

        cache.cacheCleanupManager.cleanCache();
        // cache cleaner should have cleaned it up
        assertEquals(0, cache.count());
        // stale keys count should ne decremented.
        assertEquals(0, cache.cacheCleanupManager.getStaleKeysCount().get());
    }

    private DirectoryReader getReader(IndexWriter writer, ShardId shardId) throws IOException {
        return OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), shardId);
    }

    private IndicesRequestCache getIndicesRequestCache(Settings settings) {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        return new IndicesRequestCache(settings, (shardId -> {
            IndexService indexService = null;
            try {
                indexService = indicesService.indexServiceSafe(shardId.getIndex());
            } catch (IndexNotFoundException ex) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(), threadPool);
    }

    private Loader getLoader(DirectoryReader reader) {
        return new Loader(reader, 0);
    }

    private IndicesService.IndexShardCacheEntity getEntity(IndexShard indexShard) {
        return new IndicesService.IndexShardCacheEntity(indexShard);
    }

    private BytesReference getTermBytes() throws IOException {
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        return XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
    }

    private String getReaderCacheKeyId(DirectoryReader reader) {
        OpenSearchDirectoryReader.DelegatingCacheHelper delegatingCacheHelper = (OpenSearchDirectoryReader.DelegatingCacheHelper) reader
            .getReaderCacheHelper();
        return delegatingCacheHelper.getDelegatingCacheKey().getId();
    }

    public void testEviction() throws Exception {
        final ByteSizeValue size;
        {
            threadPool = getThreadPool();
            cache = getIndicesRequestCache(Settings.EMPTY);
            writer.addDocument(newDoc(0, "foo"));
            DirectoryReader reader = getReader(writer, indexShard.shardId());
            writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
            DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));

            BytesReference value1 = cache.getOrCompute(getEntity(indexShard), getLoader(reader), reader, getTermBytes());
            assertEquals("foo", value1.streamInput().readString());
            BytesReference value2 = cache.getOrCompute(getEntity(indexShard), getLoader(secondReader), secondReader, getTermBytes());
            assertEquals("bar", value2.streamInput().readString());
            size = indexShard.requestCache().stats().getMemorySize();
            IOUtils.close(reader, secondReader, writer, dir, cache);
        }
        indexShard = createIndex("test1").getShard(0);
        IndicesRequestCache cache = new IndicesRequestCache(
            Settings.builder().put(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.getKey(), size.getBytes() + 1 + "b").build(),
            (shardId -> Optional.of(new IndicesService.IndexShardCacheEntity(indexShard))),
            new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(),
            threadPool
        );
        dir = newDirectory();
        writer = new IndexWriter(dir, newIndexWriterConfig());
        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = getReader(writer, indexShard.shardId());
        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = getReader(writer, indexShard.shardId());
        writer.updateDocument(new Term("id", "0"), newDoc(0, "baz"));
        DirectoryReader thirdReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));

        BytesReference value1 = cache.getOrCompute(getEntity(indexShard), getLoader(reader), reader, getTermBytes());
        assertEquals("foo", value1.streamInput().readString());
        BytesReference value2 = cache.getOrCompute(getEntity(indexShard), getLoader(secondReader), secondReader, getTermBytes());
        assertEquals("bar", value2.streamInput().readString());
        logger.info("Memory size: {}", indexShard.requestCache().stats().getMemorySize());
        BytesReference value3 = cache.getOrCompute(getEntity(indexShard), getLoader(thirdReader), thirdReader, getTermBytes());
        assertEquals("baz", value3.streamInput().readString());
        assertEquals(2, cache.count());
        assertEquals(1, indexShard.requestCache().stats().getEvictions());
        IOUtils.close(reader, secondReader, thirdReader);
    }

    public void testClearAllEntityIdentity() throws Exception {
        threadPool = getThreadPool();
        cache = getIndicesRequestCache(Settings.EMPTY);
        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = getReader(writer, indexShard.shardId());
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = getReader(writer, indexShard.shardId());
        IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader secondLoader = new Loader(secondReader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "baz"));
        DirectoryReader thirdReader = getReader(writer, indexShard.shardId());
        ;
        IndicesService.IndexShardCacheEntity thirddEntity = new IndicesService.IndexShardCacheEntity(createIndex("test1").getShard(0));
        Loader thirdLoader = new Loader(thirdReader, 0);

        BytesReference value1 = cache.getOrCompute(entity, loader, reader, getTermBytes());
        assertEquals("foo", value1.streamInput().readString());
        BytesReference value2 = cache.getOrCompute(secondEntity, secondLoader, secondReader, getTermBytes());
        assertEquals("bar", value2.streamInput().readString());
        logger.info("Memory size: {}", indexShard.requestCache().stats().getMemorySize());
        BytesReference value3 = cache.getOrCompute(thirddEntity, thirdLoader, thirdReader, getTermBytes());
        assertEquals("baz", value3.streamInput().readString());
        assertEquals(3, cache.count());
        RequestCacheStats requestCacheStats = entity.stats().stats();
        requestCacheStats.add(thirddEntity.stats().stats());
        final long hitCount = requestCacheStats.getHitCount();
        // clear all for the indexShard Idendity even though is't still open
        cache.clear(randomFrom(entity, secondEntity));
        cache.cacheCleanupManager.cleanCache();
        assertEquals(1, cache.count());
        // third has not been validated since it's a different identity
        value3 = cache.getOrCompute(thirddEntity, thirdLoader, thirdReader, getTermBytes());
        requestCacheStats = entity.stats().stats();
        requestCacheStats.add(thirddEntity.stats().stats());
        assertEquals(hitCount + 1, requestCacheStats.getHitCount());
        assertEquals("baz", value3.streamInput().readString());

        IOUtils.close(reader, secondReader, thirdReader);
    }

    public Iterable<Field> newDoc(int id, String value) {
        return Arrays.asList(
            newField("id", Integer.toString(id), StringField.TYPE_STORED),
            newField("value", value, StringField.TYPE_STORED)
        );
    }

    private static class Loader implements CheckedSupplier<BytesReference, IOException> {

        private final DirectoryReader reader;
        private final int id;
        public boolean loadedFromCache = true;

        Loader(DirectoryReader reader, int id) {
            super();
            this.reader = reader;
            this.id = id;
        }

        @Override
        public BytesReference get() {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                TopDocs topDocs = searcher.search(new TermQuery(new Term("id", Integer.toString(id))), 1);
                assertEquals(1, topDocs.totalHits.value);
                Document document = reader.storedFields().document(topDocs.scoreDocs[0].doc);
                out.writeString(document.get("value"));
                loadedFromCache = false;
                return out.bytes();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void testInvalidate() throws Exception {
        threadPool = getThreadPool();
        IndicesRequestCache cache = getIndicesRequestCache(Settings.EMPTY);
        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = getReader(writer, indexShard.shardId());

        // initial cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, getTermBytes());
        assertEquals("foo", value.streamInput().readString());
        ShardRequestCache requestCacheStats = entity.stats();
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());

        // cache hit
        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, getTermBytes());
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = entity.stats();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // load again after invalidate
        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        cache.invalidate(entity, reader, getTermBytes());
        value = cache.getOrCompute(entity, loader, reader, getTermBytes());
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = entity.stats();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // release
        if (randomBoolean()) {
            reader.close();
        } else {
            indexShard.close("test", true, true); // closed shard but reader is still open
            cache.clear(entity);
        }
        cache.cacheCleanupManager.cleanCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(reader);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testEqualsKey() throws IOException {
        ShardId shardId = new ShardId("foo", "bar", 1);
        ShardId shardId1 = new ShardId("foo1", "bar1", 2);
        IndexReader reader1 = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), shardId);
        String rKey1 = ((OpenSearchDirectoryReader) reader1).getDelegatingCacheHelper().getDelegatingCacheKey().getId();
        writer.addDocument(new Document());
        IndexReader reader2 = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), shardId);
        String rKey2 = ((OpenSearchDirectoryReader) reader2).getDelegatingCacheHelper().getDelegatingCacheKey().getId();
        IOUtils.close(reader1, reader2, writer, dir);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);
        IndicesRequestCache.Key key1 = new IndicesRequestCache.Key(shardId, new TestBytesReference(1), rKey1);
        IndicesRequestCache.Key key2 = new IndicesRequestCache.Key(shardId, new TestBytesReference(1), rKey1);
        IndicesRequestCache.Key key3 = new IndicesRequestCache.Key(shardId1, new TestBytesReference(1), rKey1);
        IndicesRequestCache.Key key4 = new IndicesRequestCache.Key(shardId, new TestBytesReference(1), rKey2);
        IndicesRequestCache.Key key5 = new IndicesRequestCache.Key(shardId, new TestBytesReference(2), rKey2);
        String s = "Some other random object";
        assertEquals(key1, key1);
        assertEquals(key1, key2);
        assertNotEquals(key1, null);
        assertNotEquals(key1, s);
        assertNotEquals(key1, key3);
        assertNotEquals(key1, key4);
        assertNotEquals(key1, key5);
    }

    public void testSerializationDeserializationOfCacheKey() throws Exception {
        IndicesService.IndexShardCacheEntity shardCacheEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        String readerCacheKeyId = UUID.randomUUID().toString();
        IndicesRequestCache.Key key1 = new IndicesRequestCache.Key(indexShard.shardId(), getTermBytes(), readerCacheKeyId);
        BytesReference bytesReference = null;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            key1.writeTo(out);
            bytesReference = out.bytes();
        }
        StreamInput in = bytesReference.streamInput();

        IndicesRequestCache.Key key2 = new IndicesRequestCache.Key(in);

        assertEquals(readerCacheKeyId, key2.readerCacheKeyId);
        assertEquals(((IndexShard) shardCacheEntity.getCacheIdentity()).shardId(), key2.shardId);
        assertEquals(getTermBytes(), key2.value);
    }

    private class TestBytesReference extends AbstractBytesReference {

        int dummyValue;

        TestBytesReference(int dummyValue) {
            this.dummyValue = dummyValue;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof TestBytesReference && this.dummyValue == ((TestBytesReference) other).dummyValue;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + dummyValue;
            return result;
        }

        @Override
        public byte get(int index) {
            return 0;
        }

        @Override
        public int length() {
            return 0;
        }

        @Override
        public BytesReference slice(int from, int length) {
            return null;
        }

        @Override
        public BytesRef toBytesRef() {
            return null;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public boolean isFragment() {
            return false;
        }
    }
}
