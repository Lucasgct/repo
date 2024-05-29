/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.Rounding;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.DateRangeIncludingNowQuery;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregator;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceConfig;
import org.opensearch.search.aggregations.bucket.composite.RoundingValuesSource;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Utility class to help rewrite aggregations into filters.
 * Instead of aggregation collects documents one by one, filter may count all documents that match in one pass.
 * <p>
 * Currently supported rewrite:
 * <ul>
 *  <li> date histogram : date range filter.
 *   Applied: DateHistogramAggregator, AutoDateHistogramAggregator, CompositeAggregator </li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class FastFilterRewriteHelper {

    private FastFilterRewriteHelper() {}

    private static final Logger logger = LogManager.getLogger(FastFilterRewriteHelper.class);

    private static final Map<Class<?>, Function<Query, Query>> queryWrappers;

    // Initialize the wrapper map for unwrapping the query
    static {
        queryWrappers = new HashMap<>();
        queryWrappers.put(ConstantScoreQuery.class, q -> ((ConstantScoreQuery) q).getQuery());
        queryWrappers.put(FunctionScoreQuery.class, q -> ((FunctionScoreQuery) q).getSubQuery());
        queryWrappers.put(DateRangeIncludingNowQuery.class, q -> ((DateRangeIncludingNowQuery) q).getQuery());
        queryWrappers.put(IndexOrDocValuesQuery.class, q -> ((IndexOrDocValuesQuery) q).getIndexQuery());
    }

    /**
     * Recursively unwraps query into the concrete form
     * for applying the optimization
     */
    private static Query unwrapIntoConcreteQuery(Query query) {
        while (queryWrappers.containsKey(query.getClass())) {
            query = queryWrappers.get(query.getClass()).apply(query);
        }

        return query;
    }

    /**
     * Finds the global min and max bounds of the field for the shard across all segments
     *
     * @return null if the field is empty or not indexed
     */
    private static long[] getShardBounds(final SearchContext context, final String fieldName) throws IOException {
        final List<LeafReaderContext> leaves = context.searcher().getIndexReader().leaves();
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (LeafReaderContext leaf : leaves) {
            final PointValues values = leaf.reader().getPointValues(fieldName);
            if (values != null) {
                min = Math.min(min, NumericUtils.sortableBytesToLong(values.getMinPackedValue(), 0));
                max = Math.max(max, NumericUtils.sortableBytesToLong(values.getMaxPackedValue(), 0));
            }
        }

        if (min == Long.MAX_VALUE || max == Long.MIN_VALUE) {
            return null;
        }
        return new long[] { min, max };
    }

    /**
     * Finds the min and max bounds of the field for the segment
     *
     * @return null if the field is empty or not indexed
     */
    private static long[] getSegmentBounds(final LeafReaderContext context, final String fieldName) throws IOException {
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        final PointValues values = context.reader().getPointValues(fieldName);
        if (values != null) {
            min = Math.min(min, NumericUtils.sortableBytesToLong(values.getMinPackedValue(), 0));
            max = Math.max(max, NumericUtils.sortableBytesToLong(values.getMaxPackedValue(), 0));
        }

        if (min == Long.MAX_VALUE || max == Long.MIN_VALUE) {
            return null;
        }
        return new long[] { min, max };
    }

    /**
     * Gets the min and max bounds of the field for the shard search
     * Depending on the query part, the bounds are computed differently
     *
     * @return null if the processed query not supported by the optimization
     */
    public static long[] getDateHistoAggBounds(final SearchContext context, final String fieldName) throws IOException {
        final Query cq = unwrapIntoConcreteQuery(context.query());
        if (cq instanceof PointRangeQuery) {
            final PointRangeQuery prq = (PointRangeQuery) cq;
            final long[] indexBounds = getShardBounds(context, fieldName);
            if (indexBounds == null) return null;
            return getBoundsWithRangeQuery(prq, fieldName, indexBounds);
        } else if (cq instanceof MatchAllDocsQuery) {
            return getShardBounds(context, fieldName);
        } else if (cq instanceof FieldExistsQuery) {
            // when a range query covers all values of a shard, it will be rewrite field exists query
            if (((FieldExistsQuery) cq).getField().equals(fieldName)) {
                return getShardBounds(context, fieldName);
            }
        }

        return null;
    }

    private static long[] getBoundsWithRangeQuery(PointRangeQuery prq, String fieldName, long[] indexBounds) {
        // Ensure that the query and aggregation are on the same field
        if (prq.getField().equals(fieldName)) {
            // Minimum bound for aggregation is the max between query and global
            long lower = Math.max(NumericUtils.sortableBytesToLong(prq.getLowerPoint(), 0), indexBounds[0]);
            // Maximum bound for aggregation is the min between query and global
            long upper = Math.min(NumericUtils.sortableBytesToLong(prq.getUpperPoint(), 0), indexBounds[1]);
            if (lower > upper) {
                return null;
            }
            return new long[] { lower, upper };
        }

        return null;
    }

    /**
     * Context object for fast filter optimization
     * <p>
     * Usage: first set aggregation type, then check isRewriteable, then buildFastFilter
     */
    public static class FastFilterContext {
        private boolean rewriteable = false;
        private boolean rangesBuiltAtShardLevel = false;

        private AggregationType aggregationType;
        private final SearchContext context;

        private String fieldName;
        private MappedFieldType fieldType;
        private Ranges ranges;

        // debug info related fields
        public int leaf;
        public int inner;
        public int segments;
        public int optimizedSegments;

        public FastFilterContext(SearchContext context) {
            this.context = context;
        }

        public AggregationType getAggregationType() {
            return aggregationType;
        }

        public void setAggregationType(AggregationType aggregationType) {
            this.aggregationType = aggregationType;
        }

        public boolean isRewriteable(final Object parent, final int subAggLength) {
            if (context.maxAggRewriteFilters() == 0) return false;

            boolean rewriteable = aggregationType.isRewriteable(parent, subAggLength);
            logger.debug("Fast filter rewriteable: {} for shard {}", rewriteable, context.indexShard().shardId());
            this.rewriteable = rewriteable;
            return rewriteable;
        }

        public void buildRanges(MappedFieldType fieldType) throws IOException {
            assert ranges == null : "Ranges should only be built once at shard level, but they are already built";
            this.fieldName = fieldType.name();
            this.fieldType = fieldType;
            this.ranges = this.aggregationType.buildRanges(context, fieldType);
            if (ranges != null) {
                logger.debug("Ranges built for shard {}", context.indexShard().shardId());
                rangesBuiltAtShardLevel = true;
            }
        }

        private Ranges buildRanges(LeafReaderContext leaf) throws IOException {
            Ranges ranges = this.aggregationType.buildRanges(leaf, context, fieldType);
            if (ranges != null) {
                logger.debug("Ranges built for shard {} segment {}", context.indexShard().shardId(), leaf.ord);
            }
            return ranges;
        }

        /**
         * Try to populate the bucket doc counts for aggregation
         * <p>
         * Usage: invoked at segment level — in getLeafCollector of aggregator
         */
        public boolean tryFastFilterAggregation(
            final LeafReaderContext ctx,
            final BiConsumer<Long, Long> incrementDocCount,
            final Function<Object, Long> bucketOrd
        ) throws IOException {
            this.segments++;
            if (!this.rewriteable) {
                return false;
            }

            if (ctx.reader().hasDeletions()) return false;

            PointValues values = ctx.reader().getPointValues(this.fieldName);
            if (values == null) return false;
            // only proceed if every document corresponds to exactly one point
            if (values.getDocCount() != values.size()) return false;

            NumericDocValues docCountValues = DocValues.getNumeric(ctx.reader(), DocCountFieldMapper.NAME);
            if (docCountValues.nextDoc() != NO_MORE_DOCS) {
                logger.debug(
                    "Shard {} segment {} has at least one document with _doc_count field, skip fast filter optimization",
                    this.context.indexShard().shardId(),
                    ctx.ord
                );
                return false;
            }

            // even if no ranges built at shard level, we can still perform the optimization
            // when functionally match-all at segment level
            if (!this.rangesBuiltAtShardLevel && !segmentMatchAll(this.context, ctx)) {
                return false;
            }

            Ranges ranges = this.ranges;
            if (ranges == null) {
                logger.debug(
                    "Shard {} segment {} functionally match all documents. Build the fast filter",
                    this.context.indexShard().shardId(),
                    ctx.ord
                );
                ranges = this.buildRanges(ctx);
                if (ranges == null) {
                    return false;
                }
            }

            DebugInfo debugInfo = this.aggregationType.tryFastFilterAggregation(values, ranges, incrementDocCount, bucketOrd);
            this.consumeDebugInfo(debugInfo);

            this.optimizedSegments++;
            logger.debug("Fast filter optimization applied to shard {} segment {}", this.context.indexShard().shardId(), ctx.ord);
            logger.debug("crossed leaf nodes: {}, inner nodes: {}", this.leaf, this.inner);
            return true;
        }

        private void consumeDebugInfo(DebugInfo debug) {
            leaf += debug.leaf;
            inner += debug.inner;
        }
    }

    /**
     * Different types have different pre-conditions, filter building logic, etc.
     */
    interface AggregationType {
        boolean isRewriteable(Object parent, int subAggLength);

        Ranges buildRanges(SearchContext ctx, MappedFieldType fieldType) throws IOException;

        Ranges buildRanges(LeafReaderContext leaf, SearchContext ctx, MappedFieldType fieldType) throws IOException;

        DebugInfo tryFastFilterAggregation(
            PointValues values,
            Ranges ranges,
            BiConsumer<Long, Long> incrementDocCount,
            Function<Object, Long> bucketOrd
        ) throws IOException;
    }

    /**
     * For date histogram aggregation
     */
    public static abstract class AbstractDateHistogramAggregationType implements AggregationType {
        private final MappedFieldType fieldType;
        private final boolean missing;
        private final boolean hasScript;
        private LongBounds hardBounds;

        public AbstractDateHistogramAggregationType(MappedFieldType fieldType, boolean missing, boolean hasScript) {
            this.fieldType = fieldType;
            this.missing = missing;
            this.hasScript = hasScript;
        }

        public AbstractDateHistogramAggregationType(MappedFieldType fieldType, boolean missing, boolean hasScript, LongBounds hardBounds) {
            this(fieldType, missing, hasScript);
            this.hardBounds = hardBounds;
        }

        @Override
        public boolean isRewriteable(Object parent, int subAggLength) {
            if (parent == null && subAggLength == 0 && !missing && !hasScript) {
                if (fieldType != null && fieldType instanceof DateFieldMapper.DateFieldType) {
                    return fieldType.isSearchable();
                }
            }
            return false;
        }

        @Override
        public Ranges buildRanges(SearchContext context, MappedFieldType fieldType) throws IOException {
            long[] bounds = getDateHistoAggBounds(context, fieldType.name());
            logger.debug("Bounds are {} for shard {}", bounds, context.indexShard().shardId());
            return buildRanges(context, bounds);
        }

        @Override
        public Ranges buildRanges(LeafReaderContext leaf, SearchContext context, MappedFieldType fieldType) throws IOException {
            long[] bounds = getSegmentBounds(leaf, fieldType.name());
            logger.debug("Bounds are {} for shard {} segment {}", bounds, context.indexShard().shardId(), leaf.ord);
            return buildRanges(context, bounds);
        }

        private Ranges buildRanges(SearchContext context, long[] bounds) throws IOException {
            bounds = processHardBounds(bounds);
            if (bounds == null) {
                return null;
            }
            assert bounds[0] <= bounds[1] : "Low bound should be less than high bound";

            final Rounding rounding = getRounding(bounds[0], bounds[1]);
            final OptionalLong intervalOpt = Rounding.getInterval(rounding);
            if (intervalOpt.isEmpty()) {
                return null;
            }
            final long interval = intervalOpt.getAsLong();

            // process the after key of composite agg
            processAfterKey(bounds, interval);

            return FastFilterRewriteHelper.createRangesFromAgg(
                context,
                (DateFieldMapper.DateFieldType) fieldType,
                interval,
                getRoundingPrepared(),
                bounds[0],
                bounds[1]
            );
        }

        protected abstract Rounding getRounding(final long low, final long high);

        protected abstract Rounding.Prepared getRoundingPrepared();

        protected void processAfterKey(long[] bound, long interval) {}

        protected long[] processHardBounds(long[] bounds) {
            if (bounds != null) {
                // Update min/max limit if user specified any hard bounds
                if (hardBounds != null) {
                    if (hardBounds.getMin() > bounds[0]) {
                        bounds[0] = hardBounds.getMin();
                    }
                    if (hardBounds.getMax() - 1 < bounds[1]) {
                        bounds[1] = hardBounds.getMax() - 1; // hard bounds max is exclusive
                    }
                    if (bounds[0] > bounds[1]) {
                        return null;
                    }
                }
            }
            return bounds;
        }

        public DateFieldMapper.DateFieldType getFieldType() {
            assert fieldType instanceof DateFieldMapper.DateFieldType;
            return (DateFieldMapper.DateFieldType) fieldType;
        }

        @Override
        public DebugInfo tryFastFilterAggregation(
            PointValues values,
            Ranges ranges,
            BiConsumer<Long, Long> incrementDocCount,
            Function<Object, Long> bucketOrd
        ) throws IOException {
            int size = Integer.MAX_VALUE;
            if (this instanceof CompositeAggregator.CompositeAggregationType) {
                size = ((CompositeAggregator.CompositeAggregationType) this).getSize();
            }

            DateFieldMapper.DateFieldType fieldType = getFieldType();
            BiConsumer<Integer, Integer> incrementFunc = (activeIndex, docCount) -> {
                long rangeStart = LongPoint.decodeDimension(ranges.min[activeIndex], 0);
                rangeStart = fieldType.convertNanosToMillis(rangeStart);
                long ord = getBucketOrd(bucketOrd.apply(rangeStart));
                incrementDocCount.accept(ord, (long) docCount);
            };

            return multiRangesTraverse(values.getPointTree(), ranges, incrementFunc, size);
        }
    }

    /**
     * For range aggregation
     */
    public static class RangeAggregationType implements FastFilterRewriteHelper.AggregationType {

        private final ValuesSource.Numeric source;
        private final Range[] ranges;

        public RangeAggregationType(ValuesSource.Numeric source, Range[] ranges) {
            this.source = source;
            this.ranges = ranges;
        }

        @Override
        public boolean isRewriteable(Object parent, int subAggLength) {
            if (parent == null && subAggLength == 0) {
                // don't accept values source with scripts
                if (source instanceof ValuesSource.Numeric.FieldData) {
                    // ranges are already sorted by from and then to
                    // we want ranges not overlapping with each other
                    double prevTo = ranges[0].getTo();
                    for (int i = 1; i < ranges.length; i++) {
                        if (prevTo > ranges[i].getFrom()) {
                            return false;
                        }
                        prevTo = ranges[i].getTo();
                    }
                    return true;
                }
            }
            return false;
        }

        @Override
        public Ranges buildRanges(SearchContext ctx, MappedFieldType fieldType) throws IOException {
            int byteLen = -1;
            boolean scaled = false;
            String pointType = "";
            switch (fieldType.typeName()) {
                case "half_float":
                    byteLen = 2;
                    pointType = "half_float";
                    break;
                case "integer":
                case "short":
                case "byte":
                    byteLen = 4;
                    pointType = "int";
                    break;
                case "float":
                    byteLen = 4;
                    pointType = "float";
                    break;
                case "long":
                case "date":
                case "date_nanos":
                    byteLen = 8;
                    pointType = "long";
                    break;
                case "double":
                    byteLen = 8;
                    pointType = "double";
                    break;
                case "unsigned_long":
                    byteLen = 16;
                    pointType = "big_integer";
                    break;
                case "scaled_float":
                    byteLen = 8;
                    scaled = true;
                    pointType = "long";
                    break;
            }
            if (byteLen == -1) {
                throw new IllegalArgumentException("Field type " + fieldType.name() + " is not supported");
            }

            byte[][] mins = new byte[ranges.length][];
            byte[][] maxs = new byte[ranges.length][];
            for (int i = 0; i < ranges.length; i++) {
                double rangeMin = ranges[i].getFrom();
                double rangeMax = ranges[i].getTo();

                byte[] min = new byte[byteLen];
                byte[] max = new byte[byteLen];
                // TODO any heavy operation that shouldn't be in the loop?
                switch (pointType) {
                    case "half_float":
                    case "int":
                        IntPoint.encodeDimension((int) rangeMin, min, 0);
                        IntPoint.encodeDimension((int) rangeMax, max, 0);
                        break;
                    case "float":
                    case "long":
                        if (scaled) {
                            // use reflection to see if fieldType has a method called getScalingFactor
                            // if it does, use that to scale the rangeMin and rangeMax
                            // if it doesn't, use the default scaling factor of 1000
                            double scalingFactor = 100;
                            try {
                                // Method scalingFactorMethod = fieldType.getClass().getMethod("getScalingFactor");
                                scalingFactor = (double) fieldType.getClass().getMethod("getScalingFactor").invoke(fieldType);
                            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                                logger.debug("Failed to get scaling factor from fieldType", e);
                                // TODO fall back to default aggregation method
                            }
                            rangeMin = scalingFactor * rangeMin;
                            rangeMax = scalingFactor * rangeMax;
                        }
                        LongPoint.encodeDimension((long) rangeMin, min, 0);
                        LongPoint.encodeDimension((long) rangeMax, max, 0);
                        break;
                    case "double":
                    case "big_integer":
                }

                mins[i] = min;
                maxs[i] = max;
            }

            return new Ranges(mins, maxs, byteLen);
        }

        @Override
        public Ranges buildRanges(LeafReaderContext leaf, SearchContext ctx, MappedFieldType fieldType) throws IOException {
            return buildRanges(ctx, fieldType);
        }

        @Override
        public DebugInfo tryFastFilterAggregation(
            PointValues values,
            Ranges ranges,
            BiConsumer<Long, Long> incrementDocCount,
            Function<Object, Long> bucketOrd
        ) throws IOException {
            int size = Integer.MAX_VALUE;

            BiConsumer<Integer, Integer> incrementFunc = (activeIndex, docCount) -> {
                long ord = bucketOrd.apply(activeIndex);
                incrementDocCount.accept(ord, (long) docCount);
            };

            return multiRangesTraverse(values.getPointTree(), ranges, incrementFunc, size);
        }
    }

    public static boolean isCompositeAggRewriteable(CompositeValuesSourceConfig[] sourceConfigs) {
        return sourceConfigs.length == 1 && sourceConfigs[0].valuesSource() instanceof RoundingValuesSource;
    }

    public static long getBucketOrd(long bucketOrd) {
        if (bucketOrd < 0) { // already seen
            bucketOrd = -1 - bucketOrd;
        }

        return bucketOrd;
    }

    private static boolean segmentMatchAll(SearchContext ctx, LeafReaderContext leafCtx) throws IOException {
        Weight weight = ctx.searcher().createWeight(ctx.query(), ScoreMode.COMPLETE_NO_SCORES, 1f);
        return weight != null && weight.count(leafCtx) == leafCtx.reader().numDocs();
    }

    /**
     * Creates the date ranges from date histo aggregations using its interval,
     * and min/max boundaries
     */
    private static Ranges createRangesFromAgg(
        final SearchContext context,
        final DateFieldMapper.DateFieldType fieldType,
        final long interval,
        final Rounding.Prepared preparedRounding,
        long low,
        final long high
    ) {
        // Calculate the number of buckets using range and interval
        long roundedLow = preparedRounding.round(fieldType.convertNanosToMillis(low));
        long prevRounded = roundedLow;
        int bucketCount = 0;
        while (roundedLow <= fieldType.convertNanosToMillis(high)) {
            bucketCount++;
            int maxNumFilterBuckets = context.maxAggRewriteFilters();
            if (bucketCount > maxNumFilterBuckets) {
                logger.debug("Max number of filters reached [{}], skip the fast filter optimization", maxNumFilterBuckets);
                return null;
            }
            // Below rounding is needed as the interval could return in
            // non-rounded values for something like calendar month
            roundedLow = preparedRounding.round(roundedLow + interval);
            if (prevRounded == roundedLow) break; // prevents getting into an infinite loop
            prevRounded = roundedLow;
        }

        long[][] ranges = new long[bucketCount][2];
        if (bucketCount > 0) {
            roundedLow = preparedRounding.round(fieldType.convertNanosToMillis(low));

            int i = 0;
            while (i < bucketCount) {
                // Calculate the lower bucket bound
                long lower = i == 0 ? low : fieldType.convertRoundedMillisToNanos(roundedLow);
                roundedLow = preparedRounding.round(roundedLow + interval);

                // Subtract -1 if the minimum is roundedLow as roundedLow itself
                // is included in the next bucket
                long upper = i + 1 == bucketCount ? high : fieldType.convertRoundedMillisToNanos(roundedLow) - 1;

                ranges[i][0] = lower;
                ranges[i][1] = upper;
                i++;
            }
        }

        byte[][] mins = new byte[ranges.length][];
        byte[][] maxs = new byte[ranges.length][];
        for (int i = 0; i < ranges.length; i++) {
            byte[] min = new byte[8];
            byte[] max = new byte[8];
            LongPoint.encodeDimension(ranges[i][0], min, 0);
            LongPoint.encodeDimension(ranges[i][1], max, 0);
            mins[i] = min;
            maxs[i] = max;
        }

        return new Ranges(mins, maxs, 8);
    }

    /**
     * @param maxNumNonZeroRanges the number of non-zero ranges to collect
     */
    private static DebugInfo multiRangesTraverse(
        final PointValues.PointTree tree,
        final Ranges ranges,
        final BiConsumer<Integer, Integer> incrementDocCount,
        final int maxNumNonZeroRanges
    ) throws IOException {
        DebugInfo debugInfo = new DebugInfo();
        int activeIndex = ranges.firstRangeIndex(tree.getMinPackedValue(), tree.getMaxPackedValue());
        if (activeIndex < 0) {
            logger.debug("No ranges match the query, skip the fast filter optimization");
            return debugInfo;
        }
        RangeCollectorForPointTree collector = new RangeCollectorForPointTree(incrementDocCount, maxNumNonZeroRanges, ranges, activeIndex);
        PointValues.IntersectVisitor visitor = getIntersectVisitor(collector);
        try {
            intersectWithRanges(visitor, tree, collector, debugInfo);
        } catch (CollectionTerminatedException e) {
            logger.debug("Early terminate since no more range to collect");
        }
        collector.finalizePreviousRange();

        return debugInfo;
    }

    private static final ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(8);

    private static class Ranges {
        byte[][] min;
        byte[][] max;
        int size;
        int byteLen;
        static ArrayUtil.ByteArrayComparator comparator;

        Ranges(byte[][] min, byte[][] max, int byteLen) {
            this.min = min;
            this.max = max;
            assert min.length == max.length;
            this.size = min.length;
            this.byteLen = byteLen;
            comparator = ArrayUtil.getUnsignedComparator(byteLen);
        }

        public int firstRangeIndex(byte[] globalMin, byte[] globalMax) {
            if (compareByteValue(min[0], globalMax) > 0) {
                return -1;
            }
            int i = 0;
            while (compareByteValue(max[i], globalMin) < 0) {
                i++;
                if (i >= size) {
                    return -1;
                }
            }
            return i;
        }

        public static int compareByteValue(byte[] value1, byte[] value2) {
            return comparator.compare(value1, 0, value2, 0);
        }
    }

    private static void intersectWithRanges(
        PointValues.IntersectVisitor visitor,
        PointValues.PointTree pointTree,
        RangeCollectorForPointTree collector,
        DebugInfo debug
    ) throws IOException {
        PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());

        switch (r) {
            case CELL_INSIDE_QUERY:
                collector.countNode((int) pointTree.size());
                debug.visitInner();
                break;
            case CELL_CROSSES_QUERY:
                if (pointTree.moveToChild()) {
                    do {
                        intersectWithRanges(visitor, pointTree, collector, debug);
                    } while (pointTree.moveToSibling());
                    pointTree.moveToParent();
                } else {
                    pointTree.visitDocValues(visitor);
                    debug.visitLeaf();
                }
                break;
            case CELL_OUTSIDE_QUERY:
        }
    }

    private static PointValues.IntersectVisitor getIntersectVisitor(RangeCollectorForPointTree collector) {
        return new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) throws IOException {
                // this branch should be unreachable
                throw new UnsupportedOperationException(
                    "This IntersectVisitor does not perform any actions on a " + "docID=" + docID + " node being visited"
                );
            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
                visitPoints(packedValue, collector::count);
            }

            @Override
            public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                visitPoints(packedValue, () -> {
                    for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                        collector.count();
                    }
                });
            }

            private void visitPoints(byte[] packedValue, CheckedRunnable<IOException> collect) throws IOException {
                if (Ranges.compareByteValue(packedValue, collector.activeRange[1]) > 0) {
                    // need to move to next range
                    collector.finalizePreviousRange();
                    if (collector.iterateRangeEnd(packedValue)) {
                        throw new CollectionTerminatedException();
                    }
                }

                if (pointCompare(collector.activeRange[0], collector.activeRange[1], packedValue)) {
                    collect.run();
                }
            }

            private boolean pointCompare(byte[] lower, byte[] upper, byte[] packedValue) {
                if (Ranges.compareByteValue(packedValue, lower) < 0) {
                    return false;
                }
                return Ranges.compareByteValue(packedValue, upper) <= 0;
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                byte[] rangeMin = collector.activeRange[0];
                byte[] rangeMax = collector.activeRange[1];

                if (Ranges.compareByteValue(rangeMax, minPackedValue) < 0) {
                    collector.finalizePreviousRange();
                    if (collector.iterateRangeEnd(minPackedValue)) {
                        throw new CollectionTerminatedException();
                    }
                    // compare the next range with this node's min max again
                    // new rangeMin = previous rangeMax + 1 <= min
                    rangeMax = collector.activeRange[1];
                }

                if (Ranges.compareByteValue(rangeMin, minPackedValue) > 0 || Ranges.compareByteValue(rangeMax, maxPackedValue) < 0) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                } else {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                }
            }
        };
    }

    private static class RangeCollectorForPointTree {
        private final BiConsumer<Integer, Integer> incrementRangeDocCount;
        private int counter = 0;

        private final Ranges ranges;
        private int activeIndex;
        private final byte[][] activeRange = new byte[2][8];

        private int visitedRange = 0;
        private final int maxNumNonZeroRange;

        public RangeCollectorForPointTree(
            BiConsumer<Integer, Integer> incrementRangeDocCount,
            int maxNumNonZeroRange,
            Ranges ranges,
            int activeIndex
        ) {
            this.incrementRangeDocCount = incrementRangeDocCount;
            this.maxNumNonZeroRange = maxNumNonZeroRange;
            this.ranges = ranges;
            this.activeIndex = activeIndex;
            updateActiveRange();
        }

        private void count() {
            counter++;
        }

        private void countNode(int count) {
            counter += count;
        }

        private void finalizePreviousRange() {
            if (counter > 0) {
                logger.debug("finalize previous range: {}", activeIndex);
                logger.debug("counter: {}", counter);
                incrementRangeDocCount.accept(activeIndex, counter);
                counter = 0;
            }
        }

        /**
         * @return true when iterator exhausted or collect enough non-zero ranges
         */
        private boolean iterateRangeEnd(byte[] value) {
            // the new value may not be contiguous to the previous one
            // so try to find the first next range that cross the new value
            while (Ranges.compareByteValue(activeRange[1], value) < 0) {
                if (++activeIndex >= ranges.size) {
                    return true;
                }
                updateActiveRange();
            }
            visitedRange++;
            return visitedRange > maxNumNonZeroRange;
        }

        private void updateActiveRange() {
            activeRange[0] = ranges.min[activeIndex];
            activeRange[1] = ranges.max[activeIndex];
        }
    }

    /**
     * Contains debug info of BKD traversal to show in profile
     */
    public static class DebugInfo {
        private int leaf = 0; // leaf node visited
        private int inner = 0; // inner node visited

        private void visitLeaf() {
            leaf++;
        }

        private void visitInner() {
            inner++;
        }
    }
}
