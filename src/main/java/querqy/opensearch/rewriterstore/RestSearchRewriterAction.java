/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package querqy.opensearch.rewriterstore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.xcontent.*;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;
import querqy.opensearch.QuerqyPlugin;
import querqy.opensearch.QuerqyProcessor;
import querqy.opensearch.query.QuerqyQueryBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;
import static querqy.opensearch.rewriterstore.Constants.QUERQY_SEARCH_BASE_ROUTE;


public class RestSearchRewriterAction extends BaseRestHandler{

    private static final Logger LOGGER = LogManager.getLogger(TransportPutRewriterAction.class);

    @Override
    public String getName() {
        return "Search an index with Querqy";
    }

    public static final String SEARCH_PARAMS = "searchParams";

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(RestRequest.Method.POST, QUERQY_SEARCH_BASE_ROUTE+ "/{searchParams}"+"/_search"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
    {
        final SearchRewriterRequestBuilder requestBuilder = createRequestBuilder(request, client);

        return (channel) -> requestBuilder.execute(
                new RestStatusToXContentListener<>(channel, (r) -> r.getSearchResponse().toString()));


    }

    SearchRewriterRequestBuilder createRequestBuilder(final RestRequest request, final NodeClient client) {
        String searchParams = request.param(SEARCH_PARAMS);
        if (searchParams == null) {
            throw new IllegalArgumentException("SearchRewriterRequestBuilder requires search parameters");
        }

        searchParams = searchParams.trim();
        if (searchParams.isEmpty()) {
            throw new IllegalArgumentException("SearchRewriterRequestBuilder: search parameters must not be empty");
        }

        return new SearchRewriterRequestBuilder(client, SearchRewriterAction.INSTANCE,
                new SearchRewriterRequest(searchParams, request.content()));
    }


    public static class SearchRewriterRequestBuilder
            extends ActionRequestBuilder<SearchRewriterRequest, SearchRewriterResponse> {

        public SearchRewriterRequestBuilder(final OpenSearchClient client, final SearchRewriterAction action,
                                         final SearchRewriterRequest request) {
            super(client, action, request);
        }
    }


}
