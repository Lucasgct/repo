/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.fetch.subphase.highlight.serializer;

import org.opensearch.core.common.text.Text;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.server.proto.FetchSearchResultProto;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class HighlightFieldProtobufSerializer implements HighlightFieldSerializer {

    @Override
    public HighlightField createHighLightField(InputStream inputStream) throws IOException {
        FetchSearchResultProto.SearchHit.HighlightField highlightField = FetchSearchResultProto.SearchHit.HighlightField.parseFrom(
            inputStream
        );
        String name = highlightField.getName();
        Text[] fragments = Text.EMPTY_ARRAY;
        if (highlightField.getFragmentsCount() > 0) {
            List<Text> values = new ArrayList<>();
            for (String fragment : highlightField.getFragmentsList()) {
                values.add(new Text(fragment));
            }
            fragments = values.toArray(new Text[0]);
        }
        return new HighlightField(name, fragments);
    }

}
