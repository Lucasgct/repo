/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class ZstdNotEnabledIT extends OpenSearchIntegTestCase {

    public void testZStdCodecsWithoutPluginInstalled() {

        internalCluster().startNode();
        final String index = "test-index";

        // creating index with zstd or zstd_no_dict should fail if custom-codecs plugin is not installed
        assertThrows(
            IllegalArgumentException.class,
            () -> createIndex(
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.codec", randomFrom("zstd", "zstd_no_dict"))
                    .build()
            )
        );
    }

}
