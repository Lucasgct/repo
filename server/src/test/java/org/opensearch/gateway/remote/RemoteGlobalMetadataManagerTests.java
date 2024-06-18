/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.DiffableStringMap;
import org.opensearch.cluster.metadata.IndexGraveyard;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.Metadata.XContentContext;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.model.RemoteCoordinationMetadata;
import org.opensearch.gateway.remote.model.RemoteCustomMetadata;
import org.opensearch.gateway.remote.model.RemoteGlobalMetadata;
import org.opensearch.gateway.remote.model.RemoteHashesOfConsistentSettings;
import org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadata;
import org.opensearch.gateway.remote.model.RemoteReadResult;
import org.opensearch.gateway.remote.model.RemoteTemplatesMetadata;
import org.opensearch.gateway.remote.model.RemoteTransientSettingsMetadata;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.indices.IndicesModule;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.TestCustomMetadata;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.opensearch.cluster.metadata.Metadata.isGlobalStateEquals;
import static org.opensearch.common.blobstore.stream.write.WritePriority.URGENT;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.CLUSTER_STATE_PATH_TOKEN;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.CUSTOM_DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.FORMAT_PARAMS;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.PATH_DELIMITER;
import static org.opensearch.gateway.remote.model.RemoteCoordinationMetadata.COORDINATION_METADATA;
import static org.opensearch.gateway.remote.model.RemoteCoordinationMetadata.COORDINATION_METADATA_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteCoordinationMetadataTests.getCoordinationMetadata;
import static org.opensearch.gateway.remote.model.RemoteCustomMetadata.CUSTOM_METADATA;
import static org.opensearch.gateway.remote.model.RemoteCustomMetadataTests.getCustomMetadata;
import static org.opensearch.gateway.remote.model.RemoteGlobalMetadata.GLOBAL_METADATA;
import static org.opensearch.gateway.remote.model.RemoteGlobalMetadata.GLOBAL_METADATA_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteGlobalMetadataTests.getGlobalMetadata;
import static org.opensearch.gateway.remote.model.RemoteHashesOfConsistentSettings.HASHES_OF_CONSISTENT_SETTINGS;
import static org.opensearch.gateway.remote.model.RemoteHashesOfConsistentSettings.HASHES_OF_CONSISTENT_SETTINGS_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteHashesOfConsistentSettingsTests.getHashesOfConsistentSettings;
import static org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadata.SETTING_METADATA;
import static org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadataTests.getSettings;
import static org.opensearch.gateway.remote.model.RemoteTemplatesMetadata.TEMPLATES_METADATA;
import static org.opensearch.gateway.remote.model.RemoteTemplatesMetadata.TEMPLATES_METADATA_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteTemplatesMetadataTests.getTemplatesMetadata;
import static org.opensearch.gateway.remote.model.RemoteTransientSettingsMetadata.TRANSIENT_SETTING_METADATA;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteGlobalMetadataManagerTests extends OpenSearchTestCase {
    private RemoteGlobalMetadataManager remoteGlobalMetadataManager;
    private ClusterSettings clusterSettings;
    private BlobStoreRepository blobStoreRepository;
    private BlobStoreTransferService blobStoreTransferService;
    private Compressor compressor;
    private NamedXContentRegistry xContentRegistry;
    private NamedWriteableRegistry namedWriteableRegistry;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final long METADATA_VERSION = 7331L;
    private final String CLUSTER_NAME = "test-cluster";
    private final String CLUSTER_UUID = "test-cluster-uuid";

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        blobStoreRepository = mock(BlobStoreRepository.class);
        blobStoreTransferService = mock(BlobStoreTransferService.class);
        compressor = new NoneCompressor();
        xContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
        namedWriteableRegistry = writableRegistry();
        BlobPath blobPath = new BlobPath();
        when(blobStoreRepository.getCompressor()).thenReturn(compressor);
        when(blobStoreRepository.getNamedXContentRegistry()).thenReturn(xContentRegistry);
        when(blobStoreRepository.basePath()).thenReturn(blobPath);
        remoteGlobalMetadataManager = new RemoteGlobalMetadataManager(
            clusterSettings,
            CLUSTER_NAME,
            blobStoreRepository,
            blobStoreTransferService,
            writableRegistry(),
            threadPool
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testGlobalMetadataUploadWaitTimeSetting() {
        // verify default value
        assertEquals(
            RemoteGlobalMetadataManager.GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT,
            remoteGlobalMetadataManager.getGlobalMetadataUploadTimeout()
        );

        // verify update global metadata upload timeout
        int globalMetadataUploadTimeout = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.global_metadata.upload_timeout", globalMetadataUploadTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(globalMetadataUploadTimeout, remoteGlobalMetadataManager.getGlobalMetadataUploadTimeout().seconds());
    }

    public void testGetReadMetadataAsyncAction_CoordinationMetadata() throws Exception {
        CoordinationMetadata coordinationMetadata = getCoordinationMetadata();
        String fileName = randomAlphaOfLength(10);
        RemoteCoordinationMetadata coordinationMetadataForDownload = new RemoteCoordinationMetadata(
            fileName,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            COORDINATION_METADATA_FORMAT.serialize(coordinationMetadata, fileName, compressor, FORMAT_PARAMS).streamInput()
        );
        RemoteStateTestUtil.TestCapturingListener<RemoteReadResult> listener = new RemoteStateTestUtil.TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteGlobalMetadataManager.getAsyncMetadataReadAction(
            coordinationMetadataForDownload,
            COORDINATION_METADATA,
            new LatchedActionListener<>(listener, latch)
        ).run();
        latch.await();
        assertNull(listener.failure);
        assertNotNull(listener.result);
        assertEquals(coordinationMetadata, listener.result.getObj());
        assertEquals(COORDINATION_METADATA, listener.result.getComponent());
        assertEquals(COORDINATION_METADATA, listener.result.getComponentName());
    }

    public void testGetAsyncMetadataWriteAction_CoordinationMetadata() throws Exception {
        CoordinationMetadata coordinationMetadata = getCoordinationMetadata();
        RemoteCoordinationMetadata remoteCoordinationMetadata = new RemoteCoordinationMetadata(
            coordinationMetadata,
            METADATA_VERSION,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));
        RemoteStateTestUtil.TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener =
            new RemoteStateTestUtil.TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteGlobalMetadataManager.getAsyncMetadataWriteAction(remoteCoordinationMetadata, new LatchedActionListener<>(listener, latch))
            .run();
        latch.await();
        assertNull(listener.failure);
        assertNotNull(listener.result);
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = listener.result;
        assertEquals(COORDINATION_METADATA, uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(5, pathTokens.length);
        assertEquals(RemoteClusterStateUtils.encodeString(CLUSTER_NAME), pathTokens[0]);
        assertEquals(CLUSTER_STATE_PATH_TOKEN, pathTokens[1]);
        assertEquals(CLUSTER_UUID, pathTokens[2]);
        assertEquals(GLOBAL_METADATA_PATH_TOKEN, pathTokens[3]);
        String[] splitFileName = pathTokens[4].split(DELIMITER);
        assertEquals(4, splitFileName.length);
        assertEquals(COORDINATION_METADATA, splitFileName[0]);
        assertEquals(RemoteStoreUtils.invertLong(METADATA_VERSION), splitFileName[1]);
        assertEquals(GLOBAL_METADATA_CURRENT_CODEC_VERSION, Integer.parseInt(splitFileName[3]));
    }

    public void testGetReadMetadataAsyncAction_PersistentSettings() throws Exception {
        Settings settingsMetadata = getSettings();
        String fileName = randomAlphaOfLength(10);
        RemotePersistentSettingsMetadata persistentSettings = new RemotePersistentSettingsMetadata(
            fileName,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            RemotePersistentSettingsMetadata.SETTINGS_METADATA_FORMAT.serialize(settingsMetadata, fileName, compressor, FORMAT_PARAMS)
                .streamInput()
        );
        RemoteStateTestUtil.TestCapturingListener<RemoteReadResult> listener = new RemoteStateTestUtil.TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteGlobalMetadataManager.getAsyncMetadataReadAction(
            persistentSettings,
            SETTING_METADATA,
            new LatchedActionListener<>(listener, latch)
        ).run();
        latch.await();
        assertNull(listener.failure);
        assertNotNull(listener.result);
        assertEquals(settingsMetadata, listener.result.getObj());
        assertEquals(SETTING_METADATA, listener.result.getComponent());
        assertEquals(SETTING_METADATA, listener.result.getComponentName());
    }

    public void testGetAsyncMetadataWriteAction_PersistentSettings() throws Exception {
        Settings settingsMetadata = getSettings();
        RemotePersistentSettingsMetadata persistentSettings = new RemotePersistentSettingsMetadata(
            settingsMetadata,
            METADATA_VERSION,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));
        RemoteStateTestUtil.TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener =
            new RemoteStateTestUtil.TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.getAsyncMetadataWriteAction(persistentSettings, new LatchedActionListener<>(listener, latch)).run();

        latch.await();
        assertNull(listener.failure);
        assertNotNull(listener.result);
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = listener.result;
        assertEquals(SETTING_METADATA, uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(5, pathTokens.length);
        assertEquals(RemoteClusterStateUtils.encodeString(CLUSTER_NAME), pathTokens[0]);
        assertEquals(CLUSTER_STATE_PATH_TOKEN, pathTokens[1]);
        assertEquals(CLUSTER_UUID, pathTokens[2]);
        assertEquals(GLOBAL_METADATA_PATH_TOKEN, pathTokens[3]);
        String[] splitFileName = pathTokens[4].split(DELIMITER);
        assertEquals(4, splitFileName.length);
        assertEquals(SETTING_METADATA, splitFileName[0]);
        assertEquals(RemoteStoreUtils.invertLong(METADATA_VERSION), splitFileName[1]);
        assertEquals(GLOBAL_METADATA_CURRENT_CODEC_VERSION, Integer.parseInt(splitFileName[3]));
    }

    public void testGetReadMetadataAsyncAction_TransientSettings() throws Exception {
        Settings settingsMetadata = getSettings();
        String fileName = randomAlphaOfLength(10);
        RemoteTransientSettingsMetadata transientSettings = new RemoteTransientSettingsMetadata(
            fileName,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            RemoteTransientSettingsMetadata.SETTINGS_METADATA_FORMAT.serialize(settingsMetadata, fileName, compressor, FORMAT_PARAMS)
                .streamInput()
        );
        RemoteStateTestUtil.TestCapturingListener<RemoteReadResult> listener = new RemoteStateTestUtil.TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteGlobalMetadataManager.getAsyncMetadataReadAction(
            transientSettings,
            TRANSIENT_SETTING_METADATA,
            new LatchedActionListener<>(listener, latch)
        ).run();
        latch.await();
        assertNull(listener.failure);
        assertNotNull(listener.result);
        assertEquals(settingsMetadata, listener.result.getObj());
        assertEquals(TRANSIENT_SETTING_METADATA, listener.result.getComponent());
        assertEquals(TRANSIENT_SETTING_METADATA, listener.result.getComponentName());
    }

    public void testGetAsyncMetadataWriteAction_TransientSettings() throws Exception {
        Settings settingsMetadata = getSettings();
        RemoteTransientSettingsMetadata transientSettings = new RemoteTransientSettingsMetadata(
            settingsMetadata,
            METADATA_VERSION,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));
        RemoteStateTestUtil.TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener =
            new RemoteStateTestUtil.TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.getAsyncMetadataWriteAction(transientSettings, new LatchedActionListener<>(listener, latch)).run();
        latch.await();
        assertNull(listener.failure);
        assertNotNull(listener.result);
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = listener.result;
        assertEquals(TRANSIENT_SETTING_METADATA, uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(5, pathTokens.length);
        assertEquals(RemoteClusterStateUtils.encodeString(CLUSTER_NAME), pathTokens[0]);
        assertEquals(CLUSTER_STATE_PATH_TOKEN, pathTokens[1]);
        assertEquals(CLUSTER_UUID, pathTokens[2]);
        assertEquals(GLOBAL_METADATA_PATH_TOKEN, pathTokens[3]);
        String[] splitFileName = pathTokens[4].split(DELIMITER);
        assertEquals(4, splitFileName.length);
        assertEquals(TRANSIENT_SETTING_METADATA, splitFileName[0]);
        assertEquals(RemoteStoreUtils.invertLong(METADATA_VERSION), splitFileName[1]);
        assertEquals(GLOBAL_METADATA_CURRENT_CODEC_VERSION, Integer.parseInt(splitFileName[3]));
    }

    public void testGetReadMetadataAsyncAction_HashesOfConsistentSettings() throws Exception {
        DiffableStringMap hashesOfConsistentSettings = getHashesOfConsistentSettings();
        String fileName = randomAlphaOfLength(10);
        RemoteHashesOfConsistentSettings hashesOfConsistentSettingsForDownload = new RemoteHashesOfConsistentSettings(
            fileName,
            CLUSTER_UUID,
            compressor
        );
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            HASHES_OF_CONSISTENT_SETTINGS_FORMAT.serialize(hashesOfConsistentSettings, fileName, compressor).streamInput()
        );
        RemoteStateTestUtil.TestCapturingListener<RemoteReadResult> listener = new RemoteStateTestUtil.TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteGlobalMetadataManager.getAsyncMetadataReadAction(
            hashesOfConsistentSettingsForDownload,
            HASHES_OF_CONSISTENT_SETTINGS,
            new LatchedActionListener<>(listener, latch)
        ).run();
        latch.await();
        assertNull(listener.failure);
        assertNotNull(listener.result);
        assertEquals(hashesOfConsistentSettings, listener.result.getObj());
        assertEquals(HASHES_OF_CONSISTENT_SETTINGS, listener.result.getComponent());
        assertEquals(HASHES_OF_CONSISTENT_SETTINGS, listener.result.getComponentName());
    }

    public void testGetAsyncMetadataWriteAction_HashesOfConsistentSettings() throws Exception {
        DiffableStringMap hashesOfConsistentSettings = getHashesOfConsistentSettings();
        RemoteHashesOfConsistentSettings hashesOfConsistentSettingsForUpload = new RemoteHashesOfConsistentSettings(
            hashesOfConsistentSettings,
            METADATA_VERSION,
            CLUSTER_UUID,
            compressor
        );
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));
        RemoteStateTestUtil.TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener =
            new RemoteStateTestUtil.TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
            hashesOfConsistentSettingsForUpload,
            new LatchedActionListener<>(listener, latch)
        ).run();
        latch.await();
        assertNull(listener.failure);
        assertNotNull(listener.result);
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = listener.result;
        assertEquals(HASHES_OF_CONSISTENT_SETTINGS, uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(5, pathTokens.length);
        assertEquals(RemoteClusterStateUtils.encodeString(CLUSTER_NAME), pathTokens[0]);
        assertEquals(CLUSTER_STATE_PATH_TOKEN, pathTokens[1]);
        assertEquals(CLUSTER_UUID, pathTokens[2]);
        assertEquals(GLOBAL_METADATA_PATH_TOKEN, pathTokens[3]);
        String[] splitFileName = pathTokens[4].split(DELIMITER);
        assertEquals(4, splitFileName.length);
        assertEquals(HASHES_OF_CONSISTENT_SETTINGS, splitFileName[0]);
        assertEquals(RemoteStoreUtils.invertLong(METADATA_VERSION), splitFileName[1]);
        assertEquals(GLOBAL_METADATA_CURRENT_CODEC_VERSION, Integer.parseInt(splitFileName[3]));
    }

    public void testGetReadMetadataAsyncAction_TemplatesMetadata() throws Exception {
        TemplatesMetadata templatesMetadata = getTemplatesMetadata();
        String fileName = randomAlphaOfLength(10);
        RemoteTemplatesMetadata templatesMetadataForDownload = new RemoteTemplatesMetadata(
            fileName,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            TEMPLATES_METADATA_FORMAT.serialize(templatesMetadata, fileName, compressor, FORMAT_PARAMS).streamInput()
        );
        RemoteStateTestUtil.TestCapturingListener<RemoteReadResult> listener = new RemoteStateTestUtil.TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.getAsyncMetadataReadAction(
            templatesMetadataForDownload,
            TEMPLATES_METADATA,
            new LatchedActionListener<>(listener, latch)
        ).run();
        latch.await();
        assertNull(listener.failure);
        assertNotNull(listener.result);
        assertEquals(templatesMetadata, listener.result.getObj());
        assertEquals(TEMPLATES_METADATA, listener.result.getComponent());
        assertEquals(TEMPLATES_METADATA, listener.result.getComponentName());
    }

    public void testGetAsyncMetadataWriteAction_TemplatesMetadata() throws Exception {
        TemplatesMetadata templatesMetadata = getTemplatesMetadata();
        RemoteTemplatesMetadata templateMetadataForUpload = new RemoteTemplatesMetadata(
            templatesMetadata,
            METADATA_VERSION,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));
        RemoteStateTestUtil.TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener =
            new RemoteStateTestUtil.TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.getAsyncMetadataWriteAction(templateMetadataForUpload, new LatchedActionListener<>(listener, latch))
            .run();
        latch.await();
        assertNull(listener.failure);
        assertNotNull(listener.result);
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = listener.result;
        assertEquals(TEMPLATES_METADATA, uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(5, pathTokens.length);
        assertEquals(RemoteClusterStateUtils.encodeString(CLUSTER_NAME), pathTokens[0]);
        assertEquals(CLUSTER_STATE_PATH_TOKEN, pathTokens[1]);
        assertEquals(CLUSTER_UUID, pathTokens[2]);
        assertEquals(GLOBAL_METADATA_PATH_TOKEN, pathTokens[3]);
        String[] splitFileName = pathTokens[4].split(DELIMITER);
        assertEquals(4, splitFileName.length);
        assertEquals(TEMPLATES_METADATA, splitFileName[0]);
        assertEquals(RemoteStoreUtils.invertLong(METADATA_VERSION), splitFileName[1]);
        assertEquals(GLOBAL_METADATA_CURRENT_CODEC_VERSION, Integer.parseInt(splitFileName[3]));
    }

    public void testGetReadMetadataAsyncAction_CustomMetadata() throws Exception {
        Metadata.Custom customMetadata = getCustomMetadata();
        String fileName = randomAlphaOfLength(10);
        RemoteCustomMetadata customMetadataForDownload = new RemoteCustomMetadata(
            fileName,
            IndexGraveyard.TYPE,
            CLUSTER_UUID,
            compressor,
            namedWriteableRegistry
        );
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            customMetadataForDownload.customBlobStoreFormat.serialize(customMetadata, fileName, compressor).streamInput()
        );
        RemoteStateTestUtil.TestCapturingListener<RemoteReadResult> listener = new RemoteStateTestUtil.TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.getAsyncMetadataReadAction(
            customMetadataForDownload,
            IndexGraveyard.TYPE,
            new LatchedActionListener<>(listener, latch)
        ).run();
        latch.await();
        assertNull(listener.failure);
        assertNotNull(listener.result);
        assertEquals(customMetadata, listener.result.getObj());
        assertEquals(CUSTOM_METADATA, listener.result.getComponent());
        assertEquals(IndexGraveyard.TYPE, listener.result.getComponentName());
    }

    public void testGetAsyncMetadataWriteAction_CustomMetadata() throws Exception {
        Metadata.Custom customMetadata = getCustomMetadata();
        RemoteCustomMetadata customMetadataForUpload = new RemoteCustomMetadata(
            customMetadata,
            IndexGraveyard.TYPE,
            METADATA_VERSION,
            CLUSTER_UUID,
            compressor,
            namedWriteableRegistry
        );
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));
        RemoteStateTestUtil.TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener =
            new RemoteStateTestUtil.TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.getAsyncMetadataWriteAction(customMetadataForUpload, new LatchedActionListener<>(listener, latch))
            .run();
        latch.await();
        assertNull(listener.failure);
        assertNotNull(listener.result);
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = listener.result;
        assertEquals(String.join(CUSTOM_DELIMITER, CUSTOM_METADATA, IndexGraveyard.TYPE), uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(5, pathTokens.length);
        assertEquals(RemoteClusterStateUtils.encodeString(CLUSTER_NAME), pathTokens[0]);
        assertEquals(CLUSTER_STATE_PATH_TOKEN, pathTokens[1]);
        assertEquals(CLUSTER_UUID, pathTokens[2]);
        assertEquals(GLOBAL_METADATA_PATH_TOKEN, pathTokens[3]);
        String[] splitFileName = pathTokens[4].split(DELIMITER);
        assertEquals(4, splitFileName.length);
        assertEquals(String.join(CUSTOM_DELIMITER, CUSTOM_METADATA, IndexGraveyard.TYPE), splitFileName[0]);
        assertEquals(RemoteStoreUtils.invertLong(METADATA_VERSION), splitFileName[1]);
        assertEquals(GLOBAL_METADATA_CURRENT_CODEC_VERSION, Integer.parseInt(splitFileName[3]));
    }

    public void testGetReadMetadataAsyncAction_GlobalMetadata() throws Exception {
        Metadata metadata = getGlobalMetadata();
        String fileName = randomAlphaOfLength(10);
        RemoteGlobalMetadata globalMetadataForDownload = new RemoteGlobalMetadata(fileName, CLUSTER_UUID, compressor, xContentRegistry);
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            GLOBAL_METADATA_FORMAT.serialize(metadata, fileName, compressor, FORMAT_PARAMS).streamInput()
        );
        RemoteStateTestUtil.TestCapturingListener<RemoteReadResult> listener = new RemoteStateTestUtil.TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.getAsyncMetadataReadAction(
            globalMetadataForDownload,
            GLOBAL_METADATA,
            new LatchedActionListener<>(listener, latch)
        ).run();
        latch.await();
        assertNull(listener.failure);
        assertNotNull(listener.result);
        assertTrue(isGlobalStateEquals(metadata, (Metadata) listener.result.getObj()));
        assertEquals(GLOBAL_METADATA, listener.result.getComponent());
        assertEquals(GLOBAL_METADATA, listener.result.getComponentName());
    }

    public void testGetReadMetadataAsyncAction_IOException() throws Exception {
        String fileName = randomAlphaOfLength(10);
        RemoteCoordinationMetadata coordinationMetadataForDownload = new RemoteCoordinationMetadata(
            fileName,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        IOException ioException = new IOException("mock test exception");
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenThrow(ioException);
        RemoteStateTestUtil.TestCapturingListener<RemoteReadResult> listener = new RemoteStateTestUtil.TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.getAsyncMetadataReadAction(
            coordinationMetadataForDownload,
            COORDINATION_METADATA,
            new LatchedActionListener<>(listener, latch)
        ).run();
        latch.await();
        assertNull(listener.result);
        assertNotNull(listener.failure);
        assertEquals(ioException, listener.failure);
    }

    public void testGetAsyncMetadataWriteAction_IOException() throws Exception {
        CoordinationMetadata coordinationMetadata = getCoordinationMetadata();
        RemoteCoordinationMetadata remoteCoordinationMetadata = new RemoteCoordinationMetadata(
            coordinationMetadata,
            METADATA_VERSION,
            CLUSTER_UUID,
            compressor,
            xContentRegistry
        );
        IOException ioException = new IOException("mock test exception");
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onFailure(ioException);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));

        RemoteStateTestUtil.TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener =
            new RemoteStateTestUtil.TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        remoteGlobalMetadataManager.getAsyncMetadataWriteAction(remoteCoordinationMetadata, new LatchedActionListener<>(listener, latch))
            .run();
        assertNull(listener.result);
        assertNotNull(listener.failure);
        assertTrue(listener.failure instanceof RemoteStateTransferException);
        assertEquals(ioException, listener.failure.getCause());
    }

    public void testGetUpdatedCustoms() {
        Map<String, Metadata.Custom> previousCustoms = Map.of(
            CustomMetadata1.TYPE,
            new CustomMetadata1("data1"),
            CustomMetadata2.TYPE,
            new CustomMetadata2("data2"),
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3")
        );
        ClusterState previousState = ClusterState.builder(new ClusterName("test-cluster"))
            .metadata(Metadata.builder().customs(previousCustoms))
            .build();

        Map<String, Metadata.Custom> currentCustoms = Map.of(
            CustomMetadata2.TYPE,
            new CustomMetadata2("data2"),
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3-changed"),
            CustomMetadata4.TYPE,
            new CustomMetadata4("data4"),
            CustomMetadata5.TYPE,
            new CustomMetadata5("data5")
        );
        ClusterState currentState = ClusterState.builder(new ClusterName("test-cluster"))
            .metadata(Metadata.builder().customs(currentCustoms))
            .build();

        DiffableUtils.MapDiff<String, Metadata.Custom, Map<String, Metadata.Custom>> customsDiff = remoteGlobalMetadataManager
            .getCustomsDiff(currentState, previousState, true, false);
        Map<String, Metadata.Custom> expectedUpserts = Map.of(
            CustomMetadata2.TYPE,
            new CustomMetadata2("data2"),
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3-changed"),
            CustomMetadata4.TYPE,
            new CustomMetadata4("data4"),
            IndexGraveyard.TYPE,
            IndexGraveyard.builder().build()
        );
        assertThat(customsDiff.getUpserts(), is(expectedUpserts));
        assertThat(customsDiff.getDeletes(), is(List.of()));

        customsDiff = remoteGlobalMetadataManager.getCustomsDiff(currentState, previousState, false, false);
        expectedUpserts = Map.of(
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3-changed"),
            CustomMetadata4.TYPE,
            new CustomMetadata4("data4")
        );
        assertThat(customsDiff.getUpserts(), is(expectedUpserts));
        assertThat(customsDiff.getDeletes(), is(List.of(CustomMetadata1.TYPE)));

        customsDiff = remoteGlobalMetadataManager.getCustomsDiff(currentState, previousState, true, true);
        expectedUpserts = Map.of(
            CustomMetadata2.TYPE,
            new CustomMetadata2("data2"),
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3-changed"),
            CustomMetadata4.TYPE,
            new CustomMetadata4("data4"),
            CustomMetadata5.TYPE,
            new CustomMetadata5("data5"),
            IndexGraveyard.TYPE,
            IndexGraveyard.builder().build()
        );
        assertThat(customsDiff.getUpserts(), is(expectedUpserts));
        assertThat(customsDiff.getDeletes(), is(List.of()));

        customsDiff = remoteGlobalMetadataManager.getCustomsDiff(currentState, previousState, false, true);
        expectedUpserts = Map.of(
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3-changed"),
            CustomMetadata4.TYPE,
            new CustomMetadata4("data4"),
            CustomMetadata5.TYPE,
            new CustomMetadata5("data5")
        );
        assertThat(customsDiff.getUpserts(), is(expectedUpserts));
        assertThat(customsDiff.getDeletes(), is(List.of(CustomMetadata1.TYPE)));

    }

    private static class CustomMetadata1 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_1";

        CustomMetadata1(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    private static class CustomMetadata2 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_2";

        CustomMetadata2(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    private static class CustomMetadata3 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_3";

        CustomMetadata3(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    private static class CustomMetadata4 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_4";

        CustomMetadata4(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    private static class CustomMetadata5 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_5";

        CustomMetadata5(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(XContentContext.API);
        }
    }
}
