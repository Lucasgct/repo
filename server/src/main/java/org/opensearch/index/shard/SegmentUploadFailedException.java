/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import java.io.IOException;

public class SegmentUploadFailedException extends IOException {

    public SegmentUploadFailedException(String message) {
        super(message);
    }
}
