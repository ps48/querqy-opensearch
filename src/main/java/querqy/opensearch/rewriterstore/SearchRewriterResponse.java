package querqy.opensearch.rewriterstore;

import org.opensearch.action.ActionResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

public class SearchRewriterResponse extends ActionResponse implements StatusToXContentObject {

    private SearchResponse searchResponse;


    public SearchRewriterResponse(final StreamInput in) throws IOException {
        super(in);
        searchResponse = new SearchResponse(in);
    }

    public SearchRewriterResponse(final SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        searchResponse.writeTo(out);
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    @Override
    public RestStatus status() {
        return searchResponse.status();
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("search", searchResponse);
        builder.endObject();
        return builder;
    }
}
