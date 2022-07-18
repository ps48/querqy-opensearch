package querqy.opensearch.rewriterstore;

import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.util.Collections;
import java.util.List;

import static querqy.opensearch.rewriterstore.Constants.QUERQY_REWRITER_BASE_ROUTE;

public class RestSearchRewriterAction extends BaseRestHandler  {

    public static final String PARAM_REWRITER_ID = "rewriterId";

    @Override
    public String getName() {
        return "Get a Querqy rewriter";
    }

    @Override
    public List<RestHandler.Route> routes() {
        return Collections.singletonList(new RestHandler.Route(RestRequest.Method.GET, QUERQY_REWRITER_BASE_ROUTE+ "/{rewriterId}"));
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {

        final RestSearchRewriterAction.SearchRewriterRequestBuilder builder = createRequestBuilder(request, client);

        return (channel) -> builder.execute(
                new RestStatusToXContentListener<>(channel));
    }


    RestSearchRewriterAction.SearchRewriterRequestBuilder createRequestBuilder(final RestRequest request, final NodeClient client) {
        String rewriterId = request.param(PARAM_REWRITER_ID);
        if (rewriterId == null) {
            throw new IllegalArgumentException("RestGetRewriterAction requires rewriterId parameter");
        }

        rewriterId = rewriterId.trim();
        if (rewriterId.isEmpty()) {
            throw new IllegalArgumentException("RestGetRewriterAction: rewriterId parameter must not be empty");
        }

        return new RestSearchRewriterAction.SearchRewriterRequestBuilder(client, SearchRewriterAction.INSTANCE,
                new SearchRewriterRequest(rewriterId));
    }


    public static class SearchRewriterRequestBuilder
            extends ActionRequestBuilder<SearchRewriterRequest, SearchRewriterResponse> {

        public SearchRewriterRequestBuilder(final OpenSearchClient client, final SearchRewriterAction action,
                                            final SearchRewriterRequest request) {
            super(client, action, request);
        }

    }
}
