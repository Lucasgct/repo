/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class DecommissionFailedException extends OpenSearchException {

    private final DecommissionAttribute decommissionAttribute;

    public DecommissionFailedException(DecommissionAttribute decommissionAttribute, String msg) {
        this(decommissionAttribute, msg, null);
    }

    public DecommissionFailedException(DecommissionAttribute decommissionAttribute, String msg, Throwable cause) {
        super("[" + (decommissionAttribute == null ? "_na" : decommissionAttribute.toString()) + "] " + msg, cause);
        this.decommissionAttribute = decommissionAttribute;
    }

    public DecommissionFailedException(StreamInput in) throws IOException {
        super(in);
        decommissionAttribute = new DecommissionAttribute(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        decommissionAttribute.writeTo(out);
    }

    /**
     * Returns decommission attribute
     *
     * @return decommission attribute
     */
    public DecommissionAttribute decommissionAttribute() {
        return decommissionAttribute;
    }
}
