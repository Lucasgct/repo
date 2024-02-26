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

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.TriConsumer;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.support.ValueType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class KeywordTermsAggregatorTests extends AggregatorTestCase {
    private static final String KEYWORD_FIELD = "keyword";

    private static final List<String> dataset;
    static {
        List<String> d = new ArrayList<>(45);
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < i; j++) {
                d.add(String.valueOf(i));
            }
        }
        dataset = d;
    }

    private static Consumer<InternalMappedTerms> VERIFY_MATCH_ALL_DOCS = agg -> {
        assertEquals(9, agg.getBuckets().size());
        for (int i = 0; i < 9; i++) {
            StringTerms.Bucket bucket = (StringTerms.Bucket) agg.getBuckets().get(i);
            assertThat(bucket.getKey(), equalTo(String.valueOf(9L - i)));
            assertThat(bucket.getDocCount(), equalTo(9L - i));
        }
    };

    private static Query MATCH_ALL_DOCS_QUERY = new MatchAllDocsQuery();

    private static Query MATCH_NO_DOCS_QUERY = new MatchNoDocsQuery();

    public void testMatchNoDocs() throws IOException {
        testSearchCase(
            ADD_SORTED_FIELD_NO_STORE,
            MATCH_NO_DOCS_QUERY,
            dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD),
            agg -> assertEquals(0, agg.getBuckets().size()),
            null,        // without type hint
            DEFAULT_POST_COLLECTION
        );

        testSearchCase(
            ADD_SORTED_FIELD_NO_STORE,
            MATCH_NO_DOCS_QUERY,
            dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD),
            agg -> assertEquals(0, agg.getBuckets().size()),
            ValueType.STRING,        // with type hint
            DEFAULT_POST_COLLECTION
        );
    }

    public void testMatchAllDocs() throws IOException {
        testSearchCase(
            ADD_SORTED_FIELD_NO_STORE,
            MATCH_ALL_DOCS_QUERY,
            dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD),
            VERIFY_MATCH_ALL_DOCS,
            null,        // without type hint
            DEFAULT_POST_COLLECTION
        );

        testSearchCase(
            ADD_SORTED_FIELD_NO_STORE,
            MATCH_ALL_DOCS_QUERY,
            dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD),
            VERIFY_MATCH_ALL_DOCS,
            ValueType.STRING,        // with type hint
            DEFAULT_POST_COLLECTION
        );
    }

    public void testMatchAllDocsWithStoredValues() throws IOException {
        // aggregator.postCollection() is not required when LeafBucketCollector#termDocFreqCollector optimization is used,
        // therefore using NOOP_POST_COLLECTION
        // This also verifies that the bucket count is completed without running postCollection()

        testSearchCase(
            ADD_SORTED_FIELD_STORE,
            MATCH_ALL_DOCS_QUERY,
            dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD),
            VERIFY_MATCH_ALL_DOCS,
            null,            // without type hint
            NOOP_POST_COLLECTION
        );

        testSearchCase(
            ADD_SORTED_FIELD_STORE,
            MATCH_ALL_DOCS_QUERY,
            dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD),
            VERIFY_MATCH_ALL_DOCS,
            ValueType.STRING,        // with type hint
            NOOP_POST_COLLECTION
        );
    }

    private void testSearchCase(
        TriConsumer<Document, String, String> addField,
        Query query,
        List<String> dataset,
        Consumer<TermsAggregationBuilder> configure,
        Consumer<InternalMappedTerms> verify,
        ValueType valueType,
        Consumer<Aggregator> postCollectionConsumer
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (String value : dataset) {
                    addField.apply(document, KEYWORD_FIELD, value);
                    document.add(new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef(value)));
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name");
                if (valueType != null) {
                    aggregationBuilder.userValueTypeHint(valueType);
                }
                if (configure != null) {
                    configure.accept(aggregationBuilder);
                }

                MappedFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD);

                InternalMappedTerms rareTerms = searchAndReduce(indexSearcher, query, aggregationBuilder, keywordFieldType);
                verify.accept(rareTerms);
            }
        }
    }
}
