/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action;

import org.apache.lucene.util.SetOnce;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.collect.Map;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.identity.rest.request.AddPermissionRequest;
import org.opensearch.identity.rest.request.DeletePermissionRequest;
import org.opensearch.identity.rest.request.CheckPermissionRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests to verify the behavior of rest create user action
 */
public class RestPermissionTests extends OpenSearchTestCase {
    public void testParseCreateUserRequestWithInvalidJsonThrowsException() {
        AddPermissionAction action = new AddPermissionAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{invalid_json}"),
            XContentType.JSON
        ).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("Failed to parse create user request body"));
    }

    public void testCreateUserWithValidJson() throws Exception {
        SetOnce<Boolean> createUserCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                AddPermissionRequest req = (AddPermissionRequest) request;
                createUserCalled.set(true);
                assertThat(req.getPermission(), equalTo("test"));
            }
        }) {
            RestAddPermissionAction action = new RestAddPermissionAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("name", "test"))
                .withContent(new BytesArray("{ \"password\" : \"test\" }\n"), XContentType.JSON)
                .build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(createUserCalled.get(), equalTo(true));
        }
    }

}
