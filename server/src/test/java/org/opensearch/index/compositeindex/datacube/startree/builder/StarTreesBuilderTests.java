/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.StarTreeMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class StarTreesBuilderTests extends OpenSearchTestCase {

    private MapperService mapperService;
    private SegmentWriteState segmentWriteState;
    private DocValuesProducer docValuesProducer;
    private StarTreeMapper.StarTreeFieldType starTreeFieldType;
    private StarTreeField starTreeField;
    private Map<String, DocValuesProducer> fieldProducerMap;
    private Directory directory;

    public void setUp() throws Exception {
        super.setUp();
        mapperService = mock(MapperService.class);
        directory = newFSDirectory(createTempDir());
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LUCENE_9_11_0,
            "test_segment",
            5,
            false,
            false,
            new Lucene99Codec(),
            new HashMap<>(),
            UUID.randomUUID().toString().substring(0, 16).getBytes(StandardCharsets.UTF_8),
            new HashMap<>(),
            null
        );
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[0]);
        segmentWriteState = new SegmentWriteState(
            InfoStream.getDefault(),
            segmentInfo.dir,
            segmentInfo,
            fieldInfos,
            null,
            newIOContext(random())
        );
        docValuesProducer = mock(DocValuesProducer.class);
        StarTreeFieldConfiguration starTreeFieldConfiguration = new StarTreeFieldConfiguration(
            1,
            new HashSet<>(),
            StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP
        );
        starTreeField = new StarTreeField("star_tree", new ArrayList<>(), new ArrayList<>(), starTreeFieldConfiguration);
        starTreeFieldType = new StarTreeMapper.StarTreeFieldType("star_tree", starTreeField);
        fieldProducerMap = new HashMap<>();
        fieldProducerMap.put("field1", docValuesProducer);
    }

    public void test_buildWithNoStarTreeFields() throws IOException {
        when(mapperService.getCompositeFieldTypes()).thenReturn(new HashSet<>());

        StarTreesBuilder starTreesBuilder = new StarTreesBuilder(fieldProducerMap, segmentWriteState, mapperService);
        starTreesBuilder.build();

        verifyNoInteractions(docValuesProducer);
    }

    public void test_getSingleTreeBuilder() throws IOException {
        when(mapperService.getCompositeFieldTypes()).thenReturn(Set.of(starTreeFieldType));
        StarTreeBuilder starTreeBuilder = StarTreesBuilder.getSingleTreeBuilder(starTreeField, fieldProducerMap, segmentWriteState, mapperService);
        assertTrue(starTreeBuilder instanceof OnHeapStarTreeBuilder);
    }

    public void test_getSingleTreeBuilder_illegalArgument() {
        when(mapperService.getCompositeFieldTypes()).thenReturn(Set.of(starTreeFieldType));
        StarTreeFieldConfiguration starTreeFieldConfiguration = new StarTreeFieldConfiguration(1, new HashSet<>(), StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP);
        StarTreeField starTreeField = new StarTreeField("star_tree", new ArrayList<>(), new ArrayList<>(), starTreeFieldConfiguration);
        assertThrows(IllegalArgumentException.class, () -> StarTreesBuilder.getSingleTreeBuilder(starTreeField, fieldProducerMap, segmentWriteState, mapperService));
    }

    public void test_closeWithNoStarTreeFields() throws IOException {
        StarTreeFieldConfiguration starTreeFieldConfiguration = new StarTreeFieldConfiguration(
            1,
            new HashSet<>(),
            StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP
        );
        StarTreeField starTreeField = new StarTreeField("star_tree", new ArrayList<>(), new ArrayList<>(), starTreeFieldConfiguration);
        starTreeFieldType = new StarTreeMapper.StarTreeFieldType("star_tree", starTreeField);
        when(mapperService.getCompositeFieldTypes()).thenReturn(Set.of(starTreeFieldType));
        StarTreesBuilder starTreesBuilder = new StarTreesBuilder(fieldProducerMap, segmentWriteState, mapperService);
        starTreesBuilder.close();

        verifyNoInteractions(docValuesProducer);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        directory.close();
    }
}
