/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.scopes;

import java.security.Principal;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.action.ActionScope;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.identity.ApplicationScope;
import org.opensearch.plugins.ExtensionPointScope;

/**
 * Limitation for the scope of an application in OpenSearch
 *
 * @opensearch.experimental
 */
public interface Scope {
    ScopeEnums.ScopeNamespace getNamespace();

    ScopeEnums.ScopeArea getArea();

    String getAction();

    default String asPermissionString() {
        return getNamespace() + "." + getArea().toString() + "." + getAction();
    }

    static Scope parseScopeFromString(String scopeAsString) {

        String[] parts = scopeAsString.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid scope format: " + scopeAsString);
        }
        ScopeEnums.ScopeNamespace scopeNamespace = ScopeEnums.ScopeNamespace.fromString(parts[0]);
        ScopeEnums.ScopeArea scopeArea = ScopeEnums.ScopeArea.fromString(parts[1]);
        String action = parts[2];

        switch (scopeNamespace) {
            case ACTION:
                switch (action) {
                    case "ALL":
                        return ActionScope.ALL;
                    case "READ":
                        return ActionScope.READ;
                    default:
                        throw new UnknownScopeException(scopeAsString);
                }
            case APPLICATION:
                if (action.equals("ALL")) {
                    return ApplicationScope.SuperUserAccess;
                }
                throw new UnknownScopeException(scopeAsString);
            case EXTENSION_POINT:
                if (action.equals("ACTION")) {
                    return ExtensionPointScope.ACTION;
                }
                throw new UnknownScopeException(scopeAsString);
            default:
                throw new UnknownScopeException(scopeAsString);
        }
    }

    default boolean isScopeInNamespace(String scope) {

        return parseScopeFromString(scope).getNamespace().equals(getNamespace());
    }

    static Set<String> getApplicationScopes(Principal principal) {

        return ExtensionsManager.getExtensionManager()
            .getExtensionIdMap()
            .get(principal.getName())
            .getScopes()
            .stream()
            .filter(scope -> Scope.parseScopeFromString(scope).getNamespace() == ScopeEnums.ScopeNamespace.APPLICATION)
            .collect(Collectors.toSet());
    }

    static Set<String> getActionScopes(Principal principal) {

        return ExtensionsManager.getExtensionManager()
            .getExtensionIdMap()
            .get(principal.getName())
            .getScopes()
            .stream()
            .filter(scope -> Scope.parseScopeFromString(scope).getNamespace() == ScopeEnums.ScopeNamespace.ACTION)
            .collect(Collectors.toSet());
    }

    static Set<String> getExtensionPointScopes(Principal principal) {

        return ExtensionsManager.getExtensionManager()
            .getExtensionIdMap()
            .get(principal.getName())
            .getScopes()
            .stream()
            .filter(scope -> Scope.parseScopeFromString(scope).getNamespace() == ScopeEnums.ScopeNamespace.EXTENSION_POINT)
            .collect(Collectors.toSet());
    }
}
