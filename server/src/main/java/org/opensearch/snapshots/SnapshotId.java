/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.snapshots;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.Objects;

/**
 * SnapshotId - snapshot name + snapshot UUID
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class SnapshotId implements Comparable<SnapshotId>, Writeable, ToXContentObject {

    private static final String NAME = "name";
    private static final String UUID = "uuid";

    private final String name;
    private final String uuid;

    // Caching hash code
    private final int hashCode;

    /**
     * Constructs a new snapshot
     *
     * @param name   snapshot name
     * @param uuid   snapshot uuid
     */
    public SnapshotId(final String name, final String uuid) {
        this.name = Objects.requireNonNull(name);
        this.uuid = Objects.requireNonNull(uuid);
        this.hashCode = computeHashCode();
    }

    /**
     * Constructs a new snapshot from a input stream
     *
     * @param in  input stream
     */
    public SnapshotId(final StreamInput in) throws IOException {
        name = in.readString();
        uuid = in.readString();
        hashCode = computeHashCode();
    }

    /**
     * Returns snapshot name
     *
     * @return snapshot name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the snapshot UUID
     *
     * @return snapshot uuid
     */
    public String getUUID() {
        return uuid;
    }

    @Override
    public String toString() {
        return name + "/" + uuid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SnapshotId that = (SnapshotId) o;
        return name.equals(that.name) && uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public int compareTo(final SnapshotId other) {
        return this.name.compareTo(other.name);
    }

    private int computeHashCode() {
        return Objects.hash(name, uuid);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(uuid);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME, name);
        builder.field(UUID, uuid);
        builder.endObject();
        return builder;
    }

    public static SnapshotId fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) { // fresh parser? move to next token
            parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        String name = null;
        String uuid = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            final String currentFieldName = parser.currentName();
            parser.nextToken();
            if (NAME.equals(currentFieldName)) {
                name = parser.text();
            } else if (UUID.equals(currentFieldName)) {
                uuid = parser.text();
            } else {
                throw new IllegalArgumentException("unknown field [" + currentFieldName + "]");
            }
        }
        return new SnapshotId(name, uuid);
    }
}
