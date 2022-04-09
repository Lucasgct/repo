/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pit;

import org.apache.lucene.util.SetOnce;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.CreatePITRequest;
import org.opensearch.action.search.CreatePITResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.ValidationException;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.search.RestCreatePITAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RestCreatePitActionTests extends OpenSearchTestCase {
    public void testRestCreatePit() throws Exception {
        SetOnce<Boolean> createPitCalled = new SetOnce<>();
        RestCreatePITAction action = new RestCreatePITAction();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void createPit(CreatePITRequest request, ActionListener<CreatePITResponse> listener) {
                createPitCalled.set(true);
                assertThat(request.getKeepAlive().getStringRep(), equalTo("1m"));
                assertTrue(request.shouldAllowPartialPitCreation());
            }
        }) {
            Map<String, String> params = new HashMap<>();
            params.put("keep_alive", "1m");
            params.put("allow_partial_pit_creation", "true");
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
                .withMethod(RestRequest.Method.POST)
                .build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(createPitCalled.get(), equalTo(true));
        }
    }

    public void testNoKeepAliveThrowsException() throws Exception {
        RestCreatePITAction action = new RestCreatePITAction();
        Map<String, String> params = new HashMap<>();
        params.put("allow_partial_pit_creation", "true");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        Exception e = expectThrows(ValidationException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("Validation Failed: 1: Keep alive cannot be empty;"));
    }
}
