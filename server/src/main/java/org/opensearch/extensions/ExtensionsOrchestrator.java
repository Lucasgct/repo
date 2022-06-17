/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.FileSystemUtils;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.discovery.PluginRequest;
import org.opensearch.discovery.PluginResponse;
import org.opensearch.extensions.ExtensionsSettings.Extension;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndicesModuleNameResponse;
import org.opensearch.index.IndicesModuleRequest;
import org.opensearch.index.IndicesModuleResponse;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.node.ReportingService;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.plugins.PluginsService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

/**
 * The main class for Plugin Extensibility
 *
 * @opensearch.internal
 */
public class ExtensionsOrchestrator implements ReportingService<PluginsAndModules> {
    public static final String REQUEST_EXTENSION_ACTION_NAME = "internal:discovery/extensions";
    public static final String INDICES_EXTENSION_POINT_ACTION_NAME = "indices:internal/extensions";
    public static final String INDICES_EXTENSION_NAME_ACTION_NAME = "indices:internal/name";

    private static final Logger logger = LogManager.getLogger(ExtensionsOrchestrator.class);
    private final Path extensionsPath;
    final Set<DiscoveryExtension> extensionsSet;
    Set<DiscoveryExtension> extensionsInitializedSet;
    final Set<DiscoveryNode> extensionsNodeSet;
    TransportService transportService;

    public ExtensionsOrchestrator(Settings settings, Path extensionsPath) throws IOException {
        logger.info("ExtensionsOrchestrator initialized");
        this.extensionsPath = extensionsPath;
        this.transportService = null;
        this.extensionsSet = new HashSet<DiscoveryExtension>();
        this.extensionsInitializedSet = new HashSet<DiscoveryExtension>();
        this.extensionsNodeSet = new HashSet<DiscoveryNode>();

        /*
         * Now Discover extensions
         */
        extensionsDiscovery();

    }

    public void setTransportService(TransportService transportService) {
        this.transportService = transportService;
    }

    @Override
    public PluginsAndModules info() {
        return null;
    }

    /*
     * Load and populate all extensions
     */
    private void extensionsDiscovery() throws IOException {
        logger.info("Extensions Config Directory :" + extensionsPath.toString());
        if (!FileSystemUtils.isAccessibleDirectory(extensionsPath, logger)) {
            return;
        }
        List<Path> pluginDirs = PluginsService.findPluginDirs(extensionsPath);
        logger.info("Hi");

        Set<Extension> extensions = new HashSet<Extension>();
        logger.info(pluginDirs);
        logger.info(Paths.get("").toAbsolutePath().toString());
        if (!pluginDirs.isEmpty()) {
            try {
                extensions = readFromExtensionsYml("extensions.yml").getExtensions();
            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalStateException("Could not read from extensions.yml");
            }
        }

        for (final Path plugin : pluginDirs) {
            try {
                PluginInfo pluginInfo = PluginInfo.readFromProperties(plugin);
                logger.info("Plugin Info: " + pluginInfo);
                String pluginName = pluginInfo.getName();
                for (Extension extension : extensions) {
                    if (extension.getName().equals(pluginName)) {
                        extensionsSet.add(
                            new DiscoveryExtension(
                                extension.getName(),
                                extension.getUniqueId(),
                                // placeholder for ephemeral id, will change with POC discovery
                                extension.getUniqueId(),
                                extension.getHostName(),
                                extension.getHostAddress(),
                                new TransportAddress(TransportAddress.META_ADDRESS, Integer.parseInt(extension.getPort())),
                                new HashMap<String, String>(),
                                Version.fromString(extension.getVersion()),
                                pluginInfo
                            )
                        );
                        DiscoveryNode extensionNode = new DiscoveryNode(
                            extension.getName(),
                            new TransportAddress(InetAddress.getByName(extension.getHostAddress()), Integer.parseInt(extension.getPort())),
                            Version.fromString(extension.getVersion())
                        );
                        extensionsNodeSet.add(extensionNode);
                        break;
                    }
                }

            } catch (final IOException e) {
                throw new IllegalStateException("Could not load plugin descriptor " + plugin.getFileName(), e);
            }
        }
        logger.info("Loaded all extensions");
    }

    public void extensionsInitialize() {
        for (DiscoveryNode extensionNode : extensionsNodeSet) {
            extensionInitialize(extensionNode);
        }
    }

    private void extensionInitialize(DiscoveryNode extensionNode) {

        final TransportResponseHandler<PluginResponse> pluginResponseHandler = new TransportResponseHandler<PluginResponse>() {

            @Override
            public PluginResponse read(StreamInput in) throws IOException {
                return new PluginResponse(in);
            }

            @Override
            public void handleResponse(PluginResponse response) {
                logger.info("received {}", response);
                logger.info("##################\n" + response.getName());
                for (DiscoveryExtension extension : extensionsSet) {
                    if (extension.getName().equals(response.getName())) {
                        extensionsInitializedSet.add(extension);
                        break;
                    }
                }
            }

            @Override
            public void handleException(TransportException exp) {
                logger.debug(new ParameterizedMessage("Plugin request failed"), exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }
        };
        try {
            transportService.connectToNode(extensionNode);
            transportService.sendRequest(
                extensionNode,
                REQUEST_EXTENSION_ACTION_NAME,
                new PluginRequest(extensionNode, new ArrayList<DiscoveryExtension>(extensionsSet)),
                pluginResponseHandler
            );
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    public void onIndexModule(IndexModule indexModule) throws UnknownHostException {
        for (DiscoveryNode extensionNode : extensionsNodeSet) {
            onIndexModule(indexModule, extensionNode);
        }
    }

    private void onIndexModule(IndexModule indexModule, DiscoveryNode extensionNode) throws UnknownHostException {
        logger.info("onIndexModule index:" + indexModule.getIndex());
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        final CountDownLatch inProgressIndexNameLatch = new CountDownLatch(1);

        final TransportResponseHandler<IndicesModuleNameResponse> indicesModuleNameResponseHandler = new TransportResponseHandler<
            IndicesModuleNameResponse>() {
            @Override
            public void handleResponse(IndicesModuleNameResponse response) {
                logger.info("ACK Response" + response);
                inProgressIndexNameLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {

            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }

            @Override
            public IndicesModuleNameResponse read(StreamInput in) throws IOException {
                return new IndicesModuleNameResponse(in);
            }

        };

        final TransportResponseHandler<IndicesModuleResponse> indicesModuleResponseHandler = new TransportResponseHandler<
            IndicesModuleResponse>() {

            @Override
            public IndicesModuleResponse read(StreamInput in) throws IOException {
                return new IndicesModuleResponse(in);
            }

            @Override
            public void handleResponse(IndicesModuleResponse response) {
                logger.info("received {}", response);
                if (response.getIndexEventListener() == true) {
                    indexModule.addIndexEventListener(new IndexEventListener() {
                        @Override
                        public void beforeIndexRemoved(
                            IndexService indexService,
                            IndicesClusterStateService.AllocatedIndices.IndexRemovalReason reason
                        ) {
                            logger.info("Index Event Listener is called");
                            String indexName = indexService.index().getName();
                            logger.info("Index Name" + indexName.toString());
                            try {
                                logger.info("Sending request of index name to extension");
                                transportService.sendRequest(
                                    extensionNode,
                                    INDICES_EXTENSION_NAME_ACTION_NAME,
                                    new IndicesModuleRequest(indexModule),
                                    indicesModuleNameResponseHandler
                                );
                                /*
                                 * Making async synchronous for now.
                                 */
                                inProgressIndexNameLatch.await(100, TimeUnit.SECONDS);
                                logger.info("Received ack response from Extension");
                            } catch (Exception e) {
                                logger.error(e.toString());
                            }
                        }
                    });
                }
                inProgressLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                logger.error(new ParameterizedMessage("IndicesModuleRequest failed"), exp);
                inProgressLatch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }
        };

        try {
            logger.info("Sending request to extension");
            transportService.sendRequest(
                extensionNode,
                INDICES_EXTENSION_POINT_ACTION_NAME,
                new IndicesModuleRequest(indexModule),
                indicesModuleResponseHandler
            );
            /*
             * Making async synchronous for now.
             */
            inProgressLatch.await(100, TimeUnit.SECONDS);
            logger.info("Received response from Extension");
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    private static ExtensionsSettings readFromExtensionsYml(String filePath) throws Exception {
        logger.info(Paths.get(filePath).toAbsolutePath().toString());
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        InputStream input = ExtensionsOrchestrator.class.getResourceAsStream(filePath);
        ExtensionsSettings extensionSettings = objectMapper.readValue(input, ExtensionsSettings.class);
        return extensionSettings;
    }

}
