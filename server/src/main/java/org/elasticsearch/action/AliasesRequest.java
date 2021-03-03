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

package org.opensearch.action;

/**
 * Needs to be implemented by all {@link org.opensearch.action.ActionRequest} subclasses that relate to
 * one or more indices and one or more aliases. Meant to be used for aliases management requests (e.g. add/remove alias,
 * get aliases) that hold aliases and indices in separate fields.
 * Allows to retrieve which indices and aliases the action relates to.
 */
public interface AliasesRequest extends IndicesRequest.Replaceable {

    /**
     * Returns the array of aliases that the action relates to
     */
    String[] aliases();

    /**
     * Returns the aliases as they were originally requested, before any potential name resolution
     */
    String[] getOriginalAliases();

    /**
     * Replaces current aliases with the provided aliases.
     *
     * Sometimes aliases expressions need to be resolved to concrete aliases prior to executing the transport action.
     */
    void replaceAliases(String... aliases);

    /**
     * Returns true if wildcards expressions among aliases should be resolved, false otherwise
     */
    boolean expandAliasesWildcards();
}
