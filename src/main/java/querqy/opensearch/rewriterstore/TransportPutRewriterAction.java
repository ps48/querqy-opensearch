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

import static org.opensearch.action.ActionListener.wrap;
import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT;
import static querqy.opensearch.rewriterstore.Constants.DEFAULT_QUERQY_INDEX_NUM_REPLICAS;
import static querqy.opensearch.rewriterstore.Constants.QUERQY_INDEX_NAME;
import static querqy.opensearch.rewriterstore.Constants.SETTINGS_QUERQY_INDEX_NUM_REPLICAS;
import static querqy.opensearch.rewriterstore.PutRewriterAction.NAME;
import static querqy.opensearch.rewriterstore.RewriterConfigMapping.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.authuser.User;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.SearchHits;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import querqy.opensearch.security.UserAccessManager;
import org.opensearch.search.builder.SearchSourceBuilder;
import querqy.opensearch.settings.PluginSettings;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TransportPutRewriterAction extends HandledTransportAction<PutRewriterRequest, PutRewriterResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportPutRewriterAction.class);

    private final Client client;
    private final ClusterService clusterService;
    private final Settings settings;
    private boolean mappingsVersionChecked = false;
    private final PluginSettings pluginSettings = PluginSettings.getInstance();
    private String userStr;
    private User user;

    @Inject
    public TransportPutRewriterAction(final TransportService transportService, final ActionFilters actionFilters,
                                      final ClusterService clusterService, final Client client, final Settings settings)
    {
        super(NAME, false, transportService, actionFilters, PutRewriterRequest::new);
        this.clusterService = clusterService;
        this.client = client;
        this.settings = settings;
    }

    @Override
    protected void doExecute(final Task task, final PutRewriterRequest request,
                             final ActionListener<PutRewriterResponse> listener) {


        this.userStr = client.threadPool().getThreadContext().getTransient(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
        this.user = User.parse(userStr);
        UserAccessManager.validateUser(user);

        final IndicesAdminClient indicesClient = client.admin().indices();

        indicesClient.prepareGetMappings(QUERQY_INDEX_NAME).execute(new ActionListener<GetMappingsResponse>() {

            @Override
            @SuppressWarnings("unchecked")
            public void onResponse(final GetMappingsResponse getMappingsResponse) {
                final ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> mappings = getMappingsResponse
                        .getMappings();

                if (!mappingsVersionChecked) {

                    final Map<String, Object> properties = (Map<String, Object>) mappings.get(QUERQY_INDEX_NAME)
                        .get("querqy-rewriter").getSourceAsMap().get("properties");
                    if (!properties.containsKey("info_logging")) {
                        try {
                            update1To3(indicesClient);
                            mappingsVersionChecked = true;
                        } catch (final Exception e) {
                            listener.onFailure(e);
                            return;
                        }

                    } else if (!properties.containsKey(RewriterConfigMapping.CURRENT.getConfigStringProperty())) {
                        try {
                            update2To3(indicesClient);
                            mappingsVersionChecked = true;
                        } catch (final Exception e) {
                            listener.onFailure(e);
                            return;
                        }

                    }
                }
                try {
                    saveRewriter(task, request, listener);
                } catch (final IOException e) {
                    listener.onFailure(e);
                }

            }

            @Override
            public void onFailure(final Exception e) {
                if ((e instanceof IndexNotFoundException) || (e.getCause() instanceof IndexNotFoundException)) {

                    indicesClient.create(buildCreateQuerqyIndexRequest(indicesClient),
                            new ActionListener<CreateIndexResponse>() {

                                @Override
                                public void onResponse(final CreateIndexResponse createIndexResponse) {
                                    LOGGER.info("Created index {}", QUERQY_INDEX_NAME);
                                    mappingsVersionChecked = true;
                                    try {
                                        saveRewriter(task, request, listener);
                                    } catch (final IOException e) {
                                        listener.onFailure(e);
                                    }
                                }

                                @Override
                                public void onFailure(final Exception e) {
                                    listener.onFailure(e);
                                }
                            });

                } else {
                    listener.onFailure(e);
                }
            }
        });

    }

    protected void update1To3(final IndicesAdminClient indicesClient ) throws ExecutionException,
            InterruptedException {
        final PutMappingRequest request = new PutMappingRequest(QUERQY_INDEX_NAME).source(
                readUtf8Resource("querqy-mappings1to3.json"), XContentType.JSON
        ).type("querqy-rewriter");

        if (!indicesClient.putMapping(request).get().isAcknowledged()) {
            throw new IllegalStateException("Adding info_logging to mappings not " +
                    "acknowledged");
        }

        LOGGER.info("Added info_logging property and config_v_003 to index {}", QUERQY_INDEX_NAME);

    }

    protected void update2To3(final IndicesAdminClient indicesClient ) throws ExecutionException,
            InterruptedException {
        final PutMappingRequest request = new PutMappingRequest(QUERQY_INDEX_NAME).source(
                readUtf8Resource("querqy-mappings2to3.json"), XContentType.JSON
        ).type("querqy-rewriter");

        if (!indicesClient.putMapping(request).get().isAcknowledged()) {
            throw new IllegalStateException("Adding config_v_003 to mappings not " +
                    "acknowledged");
        }

        LOGGER.info("Added config_v_003 property to index {}", QUERQY_INDEX_NAME);

    }

    protected CreateIndexRequest buildCreateQuerqyIndexRequest(final IndicesAdminClient indicesClient) {

        final CreateIndexRequestBuilder createIndexRequestBuilder = indicesClient.prepareCreate(QUERQY_INDEX_NAME);
        final int numReplicas = settings.getAsInt(SETTINGS_QUERQY_INDEX_NUM_REPLICAS, DEFAULT_QUERQY_INDEX_NUM_REPLICAS);
        return  createIndexRequestBuilder
                .addMapping("querqy-rewriter", readUtf8Resource("querqy-mapping.json"), XContentType.JSON)
                .setSettings(Settings.builder().put("number_of_replicas", numReplicas))
                .request();
    }


    protected void saveRewriter(final Task task, final PutRewriterRequest request,
                                final ActionListener<PutRewriterResponse> listener) throws IOException {
        final IndexRequest indexRequest = buildIndexRequest(task, request);
        client.execute(IndexAction.INSTANCE, indexRequest,

                new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(final IndexResponse indexResponse) {
                        LOGGER.info("Saved rewriter {}", request.getRewriterId());
                        client.execute(NodesReloadRewriterAction.INSTANCE,
                                new NodesReloadRewriterRequest(request.getRewriterId()),
                                wrap(
                                        (reloadResponse) -> listener
                                                .onResponse(new PutRewriterResponse(indexResponse, reloadResponse)),
                                        listener::onFailure
                                ));
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        LOGGER.error("Could not save rewriter " + request.getRewriterId(), e);
                        listener.onFailure(e);
                    }
                })
        ;
    }


    private IndexRequest buildIndexRequest(final Task parentTask, final PutRewriterRequest request) throws IOException {

//        String querqyObjectId = null;
//        IndexRequestBuilder indexRequestBuilder = null;
//        SearchResponse response = null;

//        String userStr = client.threadPool().getThreadContext().getTransient(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
//        User user = User.parse(userStr);
        LOGGER.info("access" +userStr+" "+ String.join(", ", UserAccessManager.getAllAccessInfo(user)));
        LOGGER.info("accessv2" + org.apache.logging.log4j.ThreadContext.get("user"));
//        LOGGER.info("accessv3" + convertWithStream(org.apache.logging.log4j.ThreadContext.getContext()));
//        LOGGER.info("accessv4" + convertWithStream(client.threadPool().getThreadContext().getHeaders()));



        final IndexRequest indexRequest = client.prepareIndex(QUERQY_INDEX_NAME, null, request.getRewriterId())
                .setCreate(false)
                .setSource(RewriterConfigMapping.toLuceneSource(request.getContent(), request.getRewriterId(),
                        UserAccessManager.getUserTenant(user), UserAccessManager.getAllAccessInfo(user)))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .request();
        indexRequest.setParentTask(clusterService.localNode().getId(), parentTask.getId());
        return indexRequest;
    }



    private static String readUtf8Resource(final String name) {
        final Scanner scanner = new Scanner(TransportPutRewriterAction.class.getClassLoader().getResourceAsStream(name),
                Charset.forName("utf-8").name()).useDelimiter("\\A");
        return scanner.hasNext() ? scanner.next() : "";
    }

//    private SearchRequest querqyObjectSearchRequest(String rewriter_name, User user){
//        BoolQueryBuilder query = QueryBuilders.boolQuery();
//        query.filter(QueryBuilders.termsQuery(PROP_REWRITER_NAME,rewriter_name));
//        query.filter(QueryBuilders.termsQuery(PROP_TENANT, UserAccessManager.getUserTenant(user)));
//        if (!UserAccessManager.getAllAccessInfo(user).isEmpty()) {
//            query.filter(QueryBuilders.termsQuery(PROP_ACCESS, UserAccessManager.getAllAccessInfo(user)));
//        }
//
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        sourceBuilder.timeout(new TimeValue(pluginSettings.operationTimeoutMs, TimeUnit.MILLISECONDS)).size(1);
//        SearchRequest searchRequest = new SearchRequest();
//        searchRequest.indices(QUERQY_INDEX_NAME).source(sourceBuilder);
//
//        sourceBuilder.query(query);
//        return searchRequest;
//    }





}
