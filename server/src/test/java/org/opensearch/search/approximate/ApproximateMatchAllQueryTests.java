/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.search.IndexSearcher;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.TestSearchContext;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApproximateMatchAllQueryTests extends OpenSearchTestCase {

    public void testCanApproximate() throws IOException {
        ApproximateMatchAllQuery approximateMatchAllQuery = new ApproximateMatchAllQuery();
        // Fail on null searchContext
        assertFalse(approximateMatchAllQuery.canApproximate(null));

        ShardSearchRequest[] shardSearchRequest = new ShardSearchRequest[1];

        MapperService mockMapper = mock(MapperService.class);
        String sortfield = "myfield";
        MappedFieldType myFieldType = new NumberFieldMapper.NumberFieldType(sortfield, NumberFieldMapper.NumberType.LONG);
        when(mockMapper.fieldType(sortfield)).thenReturn(myFieldType);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("index").settings(settings).build();
        QueryShardContext queryShardContext = new QueryShardContext(
            0,
            new IndexSettings(indexMetadata, settings),
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            null,
            mockMapper,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        TestSearchContext searchContext = new TestSearchContext(queryShardContext) {
            @Override
            public ShardSearchRequest request() {
                return shardSearchRequest[0];
            }
        };

        // Fail if aggregations are present
        searchContext.aggregations(new SearchContextAggregations(new AggregatorFactories.Builder().build(null, null), null));
        assertFalse(approximateMatchAllQuery.canApproximate(searchContext));
        searchContext.aggregations(null);

        // Fail on missing ShardSearchRequest
        assertFalse(approximateMatchAllQuery.canApproximate(searchContext));

        // Fail if source is null or empty
        shardSearchRequest[0] = new ShardSearchRequest(null, System.currentTimeMillis(), null);
        assertFalse(approximateMatchAllQuery.canApproximate(searchContext));

        // Fail if source does not have a sort.
        SearchSourceBuilder source = new SearchSourceBuilder();
        shardSearchRequest[0].source(source);
        assertFalse(approximateMatchAllQuery.canApproximate(searchContext));

        // Still can't approximate, because the APPROXIMATE_POINT_RANGE_QUERY feature is not enabled.
        source.sort(sortfield);
        assertFalse(approximateMatchAllQuery.canApproximate(searchContext));

        // Now we can approximate!
        FeatureFlagSetter.set(FeatureFlags.APPROXIMATE_POINT_RANGE_QUERY);
        assertTrue(approximateMatchAllQuery.canApproximate(searchContext));
        assertTrue(approximateMatchAllQuery.rewrite((IndexSearcher) null) instanceof ApproximatePointRangeQuery);

        // But not if the sort field makes a decision about missing data
        source.sorts().clear();
        source.sort(new FieldSortBuilder(sortfield).missing("foo"));
        assertFalse(approximateMatchAllQuery.canApproximate(searchContext));
        assertThrows(IllegalStateException.class, () -> approximateMatchAllQuery.rewrite((IndexSearcher) null));
    }

}
