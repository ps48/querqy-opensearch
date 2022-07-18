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

import static querqy.opensearch.rewriterstore.Constants.QUERQY_INDEX_NAME;
import static org.opensearch.action.ActionListener.wrap;

import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.concurrent.TimeUnit;

public class TransportSearchRewriterAction extends HandledTransportAction<SearchRewriterRequest, SearchRewriterResponse> {

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportSearchRewriterAction(final TransportService transportService, final ActionFilters actionFilters,
                                         final ClusterService clusterService, final Client client) {
        super(SearchRewriterAction.NAME, false, transportService, actionFilters, SearchRewriterRequest::new);
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void doExecute(final Task task, final SearchRewriterRequest request,
                             final ActionListener<SearchRewriterResponse> listener) {


        final SearchRequestBuilder searchRequest =  client.prepareSearch(QUERQY_INDEX_NAME)
                .setSource(createSearchSource(request.getRewriterId()));

        searchRequest.execute(new ActionListener<SearchResponse>() {

            @Override
            public void onResponse(final SearchResponse searchResponse) {
                listener.onResponse(new SearchRewriterResponse(searchResponse));
            }

            @Override
            public void onFailure(final Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private SearchSourceBuilder createSearchSource(String rewriter_id){
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        if (rewriter_id!=null && !rewriter_id.isEmpty()){
            query.filter(QueryBuilders.termsQuery("_id",rewriter_id));
        }
        else{
            query.filter(QueryBuilders.matchAllQuery());
        }
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.timeout(new TimeValue(100L, TimeUnit.MILLISECONDS));
        sourceBuilder.query(query);
        return sourceBuilder;
    }
}
