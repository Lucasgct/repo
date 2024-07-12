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

package org.opensearch.action.admin.cluster.stats;

import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.cache.query.QueryCacheStats;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.fielddata.FieldDataStats;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.store.StoreStats;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Cluster Stats per index
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterStatsIndices implements ToXContentFragment {

    private int indexCount;
    private ShardStats shards;
    private DocsStats docs;
    private StoreStats store;
    private FieldDataStats fieldData;
    private QueryCacheStats queryCache;
    private CompletionStats completion;
    private SegmentsStats segments;
    private AnalysisStats analysis;
    private MappingStats mappings;

    public ClusterStatsIndices(List<ClusterStatsNodeResponse> nodeResponses, MappingStats mappingStats, AnalysisStats analysisStats) {
        Map<String, ShardStats> countsPerIndex = new HashMap<>();

        this.docs = new DocsStats();
        this.store = new StoreStats();
        this.fieldData = new FieldDataStats();
        this.queryCache = new QueryCacheStats();
        this.completion = new CompletionStats();
        this.segments = new SegmentsStats();

        for (ClusterStatsNodeResponse r : nodeResponses) {
            // Optimized response from the node
            if (r.getNodeIndexShardStats() != null) {
                r.getNodeIndexShardStats().indexStatsMap.forEach(
                    (index, indexCountStats) -> countsPerIndex.merge(index, indexCountStats, (v1, v2) -> {
                        v1.addStatsFrom(v2);
                        return v1;
                    })
                );

                docs.add(r.getNodeIndexShardStats().docs);
                store.add(r.getNodeIndexShardStats().store);
                fieldData.add(r.getNodeIndexShardStats().fieldData);
                queryCache.add(r.getNodeIndexShardStats().queryCache);
                completion.add(r.getNodeIndexShardStats().completion);
                segments.add(r.getNodeIndexShardStats().segments);
            } else {
                // Default response from the node
                for (org.opensearch.action.admin.indices.stats.ShardStats shardStats : r.shardsStats()) {
                    ShardStats indexShardStats = countsPerIndex.get(shardStats.getShardRouting().getIndexName());
                    if (indexShardStats == null) {
                        indexShardStats = new ShardStats();
                        countsPerIndex.put(shardStats.getShardRouting().getIndexName(), indexShardStats);
                    }

                    indexShardStats.total++;

                    CommonStats shardCommonStats = shardStats.getStats();

                    if (shardStats.getShardRouting().primary()) {
                        indexShardStats.primaries++;
                        docs.add(shardCommonStats.docs);
                    }
                    store.add(shardCommonStats.store);
                    fieldData.add(shardCommonStats.fieldData);
                    queryCache.add(shardCommonStats.queryCache);
                    completion.add(shardCommonStats.completion);
                    segments.add(shardCommonStats.segments);
                }
            }
        }

        shards = new ShardStats();
        indexCount = countsPerIndex.size();
        for (final ShardStats indexCountsCursor : countsPerIndex.values()) {
            shards.addIndexShardCount(indexCountsCursor);
        }

        this.mappings = mappingStats;
        this.analysis = analysisStats;
    }

    public int getIndexCount() {
        return indexCount;
    }

    public ShardStats getShards() {
        return this.shards;
    }

    public DocsStats getDocs() {
        return docs;
    }

    public StoreStats getStore() {
        return store;
    }

    public FieldDataStats getFieldData() {
        return fieldData;
    }

    public QueryCacheStats getQueryCache() {
        return queryCache;
    }

    public CompletionStats getCompletion() {
        return completion;
    }

    public SegmentsStats getSegments() {
        return segments;
    }

    public MappingStats getMappings() {
        return mappings;
    }

    public AnalysisStats getAnalysis() {
        return analysis;
    }

    /**
     * Inner Fields used for creating XContent and parsing
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String COUNT = "count";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.COUNT, indexCount);
        shards.toXContent(builder, params);
        docs.toXContent(builder, params);
        store.toXContent(builder, params);
        fieldData.toXContent(builder, params);
        queryCache.toXContent(builder, params);
        completion.toXContent(builder, params);
        segments.toXContent(builder, params);
        if (mappings != null) {
            mappings.toXContent(builder, params);
        }
        if (analysis != null) {
            analysis.toXContent(builder, params);
        }
        return builder;
    }

    /**
     * Inner Shard Stats
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class ShardStats implements ToXContentFragment, Writeable {

        int indices = 0;
        int total = 0;
        int primaries = 0;

        // min/max
        int minIndexShards = -1;
        int maxIndexShards = -1;
        int minIndexPrimaryShards = -1;
        int maxIndexPrimaryShards = -1;
        double minIndexReplication = -1;
        double totalIndexReplication = 0;
        double maxIndexReplication = -1;

        public ShardStats() {}

        public ShardStats(StreamInput in) throws IOException {
            indices = in.readVInt();
            total = in.readVInt();
            primaries = in.readVInt();
        }

        /**
         * number of indices in the cluster
         */
        public int getIndices() {
            return this.indices;
        }

        /**
         * total number of shards in the cluster
         */
        public int getTotal() {
            return this.total;
        }

        /**
         * total number of primary shards in the cluster
         */
        public int getPrimaries() {
            return this.primaries;
        }

        /**
         * returns how many *redundant* copies of the data the cluster holds - running with no replicas will return 0
         */
        public double getReplication() {
            if (primaries == 0) {
                return 0;
            }
            return (((double) (total - primaries)) / primaries);
        }

        /**
         * the maximum number of shards (primary+replicas) an index has
         */
        public int getMaxIndexShards() {
            return this.maxIndexShards;
        }

        /**
         * the minimum number of shards (primary+replicas) an index has
         */
        public int getMinIndexShards() {
            return this.minIndexShards;
        }

        /**
         * average number of shards (primary+replicas) across the indices
         */
        public double getAvgIndexShards() {
            if (this.indices == 0) {
                return -1;
            }
            return ((double) this.total) / this.indices;
        }

        /**
         * the maximum number of primary shards an index has
         */
        public int getMaxIndexPrimaryShards() {
            return this.maxIndexPrimaryShards;
        }

        /**
         * the minimum number of primary shards an index has
         */
        public int getMinIndexPrimaryShards() {
            return this.minIndexPrimaryShards;
        }

        /**
         * the average number primary shards across the indices
         */
        public double getAvgIndexPrimaryShards() {
            if (this.indices == 0) {
                return -1;
            }
            return ((double) this.primaries) / this.indices;
        }

        /**
         * minimum replication factor across the indices. See {@link #getReplication}
         */
        public double getMinIndexReplication() {
            return this.minIndexReplication;
        }

        /**
         * average replication factor across the indices. See {@link #getReplication}
         */
        public double getAvgIndexReplication() {
            if (indices == 0) {
                return -1;
            }
            return this.totalIndexReplication / this.indices;
        }

        /**
         * maximum replication factor across the indices. See {@link #getReplication}
         */
        public double getMaxIndexReplication() {
            return this.maxIndexReplication;
        }

        public void addIndexShardCount(ShardStats indexShardCount) {
            this.indices++;
            this.primaries += indexShardCount.primaries;
            this.total += indexShardCount.total;
            this.totalIndexReplication += indexShardCount.getReplication();
            if (this.indices == 1) {
                // first index, uninitialized.
                minIndexPrimaryShards = indexShardCount.primaries;
                maxIndexPrimaryShards = indexShardCount.primaries;
                minIndexShards = indexShardCount.total;
                maxIndexShards = indexShardCount.total;
                minIndexReplication = indexShardCount.getReplication();
                maxIndexReplication = minIndexReplication;
            } else {
                minIndexShards = Math.min(minIndexShards, indexShardCount.total);
                minIndexPrimaryShards = Math.min(minIndexPrimaryShards, indexShardCount.primaries);
                minIndexReplication = Math.min(minIndexReplication, indexShardCount.getReplication());

                maxIndexShards = Math.max(maxIndexShards, indexShardCount.total);
                maxIndexPrimaryShards = Math.max(maxIndexPrimaryShards, indexShardCount.primaries);
                maxIndexReplication = Math.max(maxIndexReplication, indexShardCount.getReplication());
            }
        }

        public void addStatsFrom(ShardStats incomingStats) {
            this.total += incomingStats.getTotal();
            this.indices += incomingStats.getIndices();
            this.primaries += incomingStats.getPrimaries();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(indices);
            out.writeVInt(total);
            out.writeVInt(primaries);
        }

        /**
         * Inner Fields used for creating XContent and parsing
         *
         * @opensearch.internal
         */
        static final class Fields {
            static final String SHARDS = "shards";
            static final String TOTAL = "total";
            static final String PRIMARIES = "primaries";
            static final String REPLICATION = "replication";
            static final String MIN = "min";
            static final String MAX = "max";
            static final String AVG = "avg";
            static final String INDEX = "index";
        }

        private void addIntMinMax(String field, int min, int max, double avg, XContentBuilder builder) throws IOException {
            builder.startObject(field);
            builder.field(Fields.MIN, min);
            builder.field(Fields.MAX, max);
            builder.field(Fields.AVG, avg);
            builder.endObject();
        }

        private void addDoubleMinMax(String field, double min, double max, double avg, XContentBuilder builder) throws IOException {
            builder.startObject(field);
            builder.field(Fields.MIN, min);
            builder.field(Fields.MAX, max);
            builder.field(Fields.AVG, avg);
            builder.endObject();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields.SHARDS);
            if (indices > 0) {

                builder.field(Fields.TOTAL, total);
                builder.field(Fields.PRIMARIES, primaries);
                builder.field(Fields.REPLICATION, getReplication());

                builder.startObject(Fields.INDEX);
                addIntMinMax(Fields.SHARDS, minIndexShards, maxIndexShards, getAvgIndexShards(), builder);
                addIntMinMax(Fields.PRIMARIES, minIndexPrimaryShards, maxIndexPrimaryShards, getAvgIndexPrimaryShards(), builder);
                addDoubleMinMax(Fields.REPLICATION, minIndexReplication, maxIndexReplication, getAvgIndexReplication(), builder);
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "total [" + total + "] primaries [" + primaries + "]";
        }
    }
}
