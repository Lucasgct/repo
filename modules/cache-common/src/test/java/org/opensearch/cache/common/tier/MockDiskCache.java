/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.serializer.Serializer;
import org.opensearch.common.cache.store.builders.ICacheBuilder;
import org.opensearch.common.cache.store.config.CacheConfig;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class MockDiskCache<K, V> implements ICache<K, V> {

    Map<K, V> cache;
    int maxSize;
    long delay;

    private final RemovalListener<K, V> removalListener;

    public MockDiskCache(int maxSize, long delay, RemovalListener<K, V> removalListener) {
        this.maxSize = maxSize;
        this.delay = delay;
        this.removalListener = removalListener;
        this.cache = new ConcurrentHashMap<K, V>();
    }

    @Override
    public V get(K key) {
        V value = cache.get(key);
        return value;
    }

    @Override
    public void put(K key, V value) {
        if (this.cache.size() >= maxSize) { // For simplification
            this.removalListener.onRemoval(new RemovalNotification<>(key, value, RemovalReason.EVICTED));
        }
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        this.cache.put(key, value);
    }

    @Override
    public V computeIfAbsent(K key, LoadAwareCacheLoader<K, V> loader) {
        V value = cache.computeIfAbsent(key, key1 -> {
            try {
                return loader.load(key);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return value;
    }

    @Override
    public void invalidate(K key) {
        this.cache.remove(key);
    }

    @Override
    public void invalidateAll() {
        this.cache.clear();
    }

    @Override
    public Iterable<K> keys() {
        return new Iterable<K>() {
            @Override
            public Iterator<K> iterator() {
                return new CacheKeyIterator<>(cache, removalListener);
            }
        };
    }

    @Override
    public long count() {
        return this.cache.size();
    }

    @Override
    public void refresh() {}

    @Override
    public void close() {

    }

    public static class MockDiskCacheFactory implements Factory {

        public static final String NAME = "mockDiskCache";
        final long delay;
        final int maxSize;

        public MockDiskCacheFactory(long delay, int maxSize) {
            this.delay = delay;
            this.maxSize = maxSize;
        }

        @Override
        @SuppressWarnings({ "unchecked" })
        public <K, V> ICache<K, V> create(CacheConfig<K, V> config, CacheType cacheType, Map<String, Factory> cacheFactories) {
            return new Builder<K, V>().setKeySerializer((Serializer<K, byte[]>) config.getKeySerializer())
                .setValueSerializer((Serializer<V, byte[]>) config.getValueSerializer())
                .setMaxSize(maxSize)
                .setDeliberateDelay(delay)
                .setRemovalListener(config.getRemovalListener())
                .build();
        }

        @Override
        public String getCacheName() {
            return NAME;
        }
    }

    public static class Builder<K, V> extends ICacheBuilder<K, V> {

        int maxSize;
        long delay;
        Serializer<K, byte[]> keySerializer;
        Serializer<V, byte[]> valueSerializer;

        @Override
        public ICache<K, V> build() {
            return new MockDiskCache<K, V>(this.maxSize, this.delay, this.getRemovalListener());
        }

        public Builder<K, V> setMaxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public Builder<K, V> setDeliberateDelay(long millis) {
            this.delay = millis;
            return this;
        }

        public Builder<K, V> setKeySerializer(Serializer<K, byte[]> keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        public Builder<K, V> setValueSerializer(Serializer<V, byte[]> valueSerializer) {
            this.valueSerializer = valueSerializer;
            return this;
        }

    }

    /**
     * Provides a iterator over keys.
     * @param <K> Type of key
     * @param <V> Type of value
     */
    class CacheKeyIterator<K, V> implements Iterator<K> {
        private final Iterator<Map.Entry<K, V>> entryIterator;
        private final Map<K, V> cache;
        private final RemovalListener<K, V> removalListener;
        private K lastKey;

        public CacheKeyIterator(Map<K, V> cache, RemovalListener<K, V> removalListener) {
            this.entryIterator = cache.entrySet().iterator();
            this.removalListener = removalListener;
            this.cache = cache;
        }

        @Override
        public boolean hasNext() {
            return entryIterator.hasNext();
        }

        @Override
        public K next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Map.Entry<K, V> entry = entryIterator.next();
            lastKey = entry.getKey();
            return lastKey;
        }

        @Override
        public void remove() {
            if (lastKey == null) {
                throw new IllegalStateException("No element to remove");
            }
            V value = cache.get(lastKey);
            cache.remove(lastKey);
            this.removalListener.onRemoval(new RemovalNotification<>(lastKey, value, RemovalReason.INVALIDATED));
            lastKey = null;
        }
    }
}
