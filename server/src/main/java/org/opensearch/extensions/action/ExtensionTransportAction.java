/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * A transport action used to proxy a transport request from extension to another extension
 *
 * @opensearch.internal
 */
public class ExtensionTransportAction extends TransportAction<ExtensionActionRequest, ExtensionActionResponse> {

    private final ExtensionsManager extensionsManager;

    public ExtensionTransportAction(
        String actionName,
        ActionFilters actionFilters,
        TransportService transportService,
        ExtensionsManager extensionsManager
    ) {
        super(actionName, actionFilters, transportService.getTaskManager());
        this.extensionsManager = extensionsManager;
    }

    @Override
    protected void doExecute(Task task, ExtensionActionRequest request, ActionListener<ExtensionActionResponse> listener) {
        try {
            listener.onResponse(extensionsManager.handleTransportRequest(request));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
