/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

/**
 * Container class to keep the details of uploaded manifest file
 */
public class RemoteUploadDetails {

    private final ClusterMetadataManifest clusterMetadataManifest;
    private final String manifestFileName;

    public RemoteUploadDetails(final ClusterMetadataManifest manifest, final String manifestFileName) {
        this.clusterMetadataManifest = manifest;
        this.manifestFileName = manifestFileName;
    }

    public ClusterMetadataManifest getClusterMetadataManifest() {
        return clusterMetadataManifest;
    }

    public String getManifestFileName() {
        return manifestFileName;
    }
}
