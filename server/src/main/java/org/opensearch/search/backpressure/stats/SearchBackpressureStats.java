/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.stats;

import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.backpressure.settings.SearchBackpressureMode;

import java.io.IOException;
import java.util.Objects;

/**
 * Stats related to search backpressure.
 */
public class SearchBackpressureStats implements ToXContentFragment, Writeable {
    private final SearchShardTaskStats searchShardTaskStats;
    private final SearchBackpressureMode mode;
    private boolean isNodeUnderDuress;
    @Nullable
    private final SearchTaskStats searchTaskStats;

    public SearchBackpressureStats(
        SearchTaskStats searchTaskStats,
        SearchShardTaskStats searchShardTaskStats,
        SearchBackpressureMode mode,
        boolean isNodeUnderDuress
    ) {
        this.searchShardTaskStats = searchShardTaskStats;
        this.mode = mode;
        this.searchTaskStats = searchTaskStats;
        this.isNodeUnderDuress = isNodeUnderDuress;
    }

    public SearchBackpressureStats(StreamInput in) throws IOException {
        searchShardTaskStats = new SearchShardTaskStats(in);
        mode = SearchBackpressureMode.fromName(in.readString());
        if (in.getVersion().onOrAfter(Version.V_2_6_0)) {
            searchTaskStats = in.readOptionalWriteable(SearchTaskStats::new);
        } else {
            searchTaskStats = null;
        }

        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            isNodeUnderDuress = in.readBoolean();
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("search_backpressure");
        if (searchTaskStats != null) {
            builder.field("search_task", searchTaskStats);
        }
        builder.field("search_shard_task", searchShardTaskStats);
        builder.field("mode", mode.getName());
        builder.field("is_node_under_duress", isNodeUnderDuress);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        searchShardTaskStats.writeTo(out);
        out.writeString(mode.getName());
        if (out.getVersion().onOrAfter(Version.V_2_6_0)) {
            out.writeOptionalWriteable(searchTaskStats);
        }

        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeBoolean(isNodeUnderDuress);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchBackpressureStats that = (SearchBackpressureStats) o;
        return mode == that.mode
            && isNodeUnderDuress == that.isNodeUnderDuress
            && Objects.equals(searchTaskStats, that.searchTaskStats)
            && Objects.equals(searchShardTaskStats, that.searchShardTaskStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchTaskStats, searchShardTaskStats, mode, isNodeUnderDuress);
    }
}
