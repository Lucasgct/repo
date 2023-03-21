/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.index.store.RemoteDirectory;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Factory for remote store lock manager
 *
 * @opensearch.internal
 */
public class RemoteStoreMDLockManagerFactory {
    private final Supplier<RepositoriesService> repositoriesService;
    public RemoteStoreMDLockManagerFactory(Supplier<RepositoriesService> repositoriesService) {
        this.repositoriesService = repositoriesService;
    }

    public RemoteStoreMDShardLockManager newLockManager(String repositoryName, String indexUUID,
                                                        String shardId) throws IOException {
        return newLockManager(repositoriesService.get(), repositoryName, indexUUID, shardId);
    }

    public static RemoteStoreMDShardLockManager newLockManager(RepositoriesService repositoriesService,
                                                               String repositoryName,
                                                               String indexUUID,
                                                               String shardId) throws IOException {
        try (Repository repository = repositoriesService.repository(repositoryName)) {
            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobPath shardLevelBlobPath = ((BlobStoreRepository) repository).basePath().add(indexUUID)
                .add(shardId).add("segments");
            RemoteDirectory shardMDLockDirectory = createRemoteBufferedOutputDirectory(repository, shardLevelBlobPath,
                "lock_files");

            return new FileBasedMDShardLockManager(shardMDLockDirectory);
        } catch (RepositoryMissingException e) {
            throw new IllegalArgumentException("Repository should be present to acquire/release lock", e);
        }
    }

    public RemoteStoreMDIndexLockManager newLockManager(String repositoryName, String indexUUID) throws IOException {
        return newLockManager(repositoriesService.get(), repositoryName, indexUUID);
    }

    public static RemoteStoreMDIndexLockManager newLockManager(RepositoriesService repositoriesService, String repositoryName,
                                                        String indexUUID) throws IOException {
        try (Repository repository = repositoriesService.repository(repositoryName)) {
            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobPath indexLevelBlobPath = ((BlobStoreRepository) repository).basePath().add(indexUUID);
            RemoteDirectory indexMDLockDirectory = createRemoteBufferedOutputDirectory(repository, indexLevelBlobPath,
                "resource_lock_files");

            return new FileBasedMDIndexLockManager(indexMDLockDirectory);
        } catch (RepositoryMissingException e) {
            throw new IllegalArgumentException("Repository should be present to acquire/release lock", e);
        }
    }

    private RemoteDirectory createRemoteDirectory(Repository repository, BlobPath commonBlobPath, String extention) {
        BlobPath extendedPath = commonBlobPath.add(extention);
        BlobContainer dataBlobContainer = ((BlobStoreRepository) repository).blobStore().blobContainer(extendedPath);
        return new RemoteDirectory(dataBlobContainer);
    }

    private static RemoteBufferedOutputDirectory createRemoteBufferedOutputDirectory(Repository repository,
                                                                                     BlobPath commonBlobPath,
                                                                                     String extention) {
        BlobPath extendedPath = commonBlobPath.add(extention);
        BlobContainer dataBlobContainer = ((BlobStoreRepository) repository).blobStore().blobContainer(extendedPath);
        return new RemoteBufferedOutputDirectory(dataBlobContainer);
    }
}
