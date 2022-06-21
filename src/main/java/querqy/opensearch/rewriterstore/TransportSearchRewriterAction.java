package querqy.opensearch.rewriterstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchRequestBuilder;
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
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import querqy.opensearch.QuerqyProcessor;
import querqy.opensearch.RewriterShardContext;
import querqy.opensearch.query.MatchingQuery;
import querqy.opensearch.query.QuerqyQueryBuilder;
import querqy.opensearch.query.Rewriter;
import querqy.opensearch.security.UserAccessManager;
import querqy.opensearch.settings.PluginSettings;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.action.ActionListener.wrap;
import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT;
import static querqy.opensearch.rewriterstore.Constants.*;
import static querqy.opensearch.rewriterstore.Constants.QUERQY_INDEX_NAME;
import static querqy.opensearch.rewriterstore.SearchRewriterAction.NAME;
import static querqy.opensearch.rewriterstore.RewriterConfigMapping.*;

public class TransportSearchRewriterAction extends HandledTransportAction<SearchRewriterRequest, SearchRewriterResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportPutRewriterAction.class);

    private final Client client;
    private final ClusterService clusterService;
    private final Settings settings;
    private boolean mappingsVersionChecked = false;
    private final PluginSettings pluginSettings = PluginSettings.getInstance();
    private String userStr;
    private User user;

    @Inject
    public TransportSearchRewriterAction(final TransportService transportService, final ActionFilters actionFilters,
                                      final ClusterService clusterService, final Client client, final Settings settings)
    {
        super(NAME, false, transportService, actionFilters, SearchRewriterRequest::new);
        this.clusterService = clusterService;
        this.client = client;
        this.settings = settings;
    }

    @Override
    protected void doExecute(final Task task, final SearchRewriterRequest request,
                             final ActionListener<SearchRewriterResponse> listener) {

        this.userStr = client.threadPool().getThreadContext().getTransient(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
        this.user = User.parse(userStr);

        final SearchRequestBuilder searchRequestBuilder = client.prepareSearch(request.getSearchParams());
        searchRequestBuilder.setQuery(request.getQuerqyQueryBuilder());

        SearchRequest searchRequest = searchRequestBuilder.request();

        client.execute(SearchAction.INSTANCE, searchRequest,
                new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(final SearchResponse searchResponse) {
                    LOGGER.info("querqy search request on index {}", request.getSearchParams());
                    listener.onResponse(new SearchRewriterResponse(searchResponse));
                }

                @Override
                public void onFailure(final Exception e) {
                    LOGGER.error("Could not search with querqy over index: " + request.getSearchParams(), e);
                    listener.onFailure(e);
                }
            });
        try {
            SearchResponse response = client.search(searchRequestBuilder.request()).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
