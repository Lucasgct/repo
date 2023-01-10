/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Contains stats of Cluster Manager Task Throttling.
 * It stores the total cumulative count of throttled tasks per task type.
 */
public class ClusterManagerThrottlingStats implements ClusterManagerTaskThrottlerListener, Writeable, ToXContentFragment {

    private Map<String, CounterMetric> throttledTasksCount;

    public ClusterManagerThrottlingStats() {
        throttledTasksCount = new ConcurrentHashMap<>();
    }

    private void incrementThrottlingCount(String type, final int counts) {
        throttledTasksCount.computeIfAbsent(type, k -> new CounterMetric()).inc(counts);
    }

    public long getThrottlingCount(String type) {
        return throttledTasksCount.get(type) == null ? 0 : throttledTasksCount.get(type).count();
    }

    public long getTotalThrottledTaskCount() {
        CounterMetric totalCount = new CounterMetric();
        throttledTasksCount.forEach((aClass, counterMetric) -> { totalCount.inc(counterMetric.count()); });
        return totalCount.count();
    }

    @Override
    public void onThrottle(String type, int counts) {
        incrementThrottlingCount(type, counts);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(throttledTasksCount.size());
        for (Map.Entry<String, CounterMetric> entry : throttledTasksCount.entrySet()) {
            out.writeString(entry.getKey());
            out.writeLong(entry.getValue().count());
        }
    }

    public ClusterManagerThrottlingStats(StreamInput in) throws IOException {
        int throttledTaskEntries = in.readInt();
        throttledTasksCount = new ConcurrentHashMap<>();
        for (int i = 0; i < throttledTaskEntries; i++) {
            String taskType = in.readString();
            long throttledTaskCount = in.readLong();
            onThrottle(taskType, (int) throttledTaskCount);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject("cluster_manager_throttling");
        builder.startObject("cluster_manager_stats");
        builder.field("TotalThrottledTasks", getTotalThrottledTaskCount());
        builder.startObject("ThrottledTasksPerTaskType");
        for (Map.Entry<String, CounterMetric> entry : throttledTasksCount.entrySet()) {
            builder.field(entry.getKey(), entry.getValue().count());
        }
        builder.endObject();
        builder.endObject();
        return builder.endObject();
    }

}
