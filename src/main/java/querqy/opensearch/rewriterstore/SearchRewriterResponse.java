package querqy.opensearch.rewriterstore;

import org.opensearch.action.ActionResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SearchRewriterResponse extends ActionResponse implements StatusToXContentObject  {

    private SearchResponse searchResponse;

    public SearchRewriterResponse(final  SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
    }

    public SearchRewriterResponse(final StreamInput in) throws IOException {
        super(in);
        searchResponse = new SearchResponse(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        searchResponse.writeTo(out);
    }

    @Override
    public RestStatus status() {
        return searchResponse.status();
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {

        builder.startObject();
        builder.field("put", searchResponse);
        builder.endObject();
        return builder;
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

}

//public class SearchRewriterResponse extends ActionResponse implements StatusToXContentObject {
//
//    private IndexResponse indexResponse;
//
//    public SearchRewriterResponse(final String value) {
//        this.strValue = value;
//    }
//
//    public SearchRewriterResponse(final StreamInput in) throws IOException {
//        super(in);
//        strValue = String.valueOf(in);
//    }
//
//    @Override
//    public void writeTo(final StreamOutput out) throws IOException {
//        out.write(strValue.getBytes(StandardCharsets.UTF_8));
//    }
//
//    @Override
//    public RestStatus status() {
//        return RestStatus.OK;
//    }
//
//    @Override
//    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
//
//        builder.startObject();
//        builder.field("put", strValue);
//        builder.endObject();
//        return builder;
//    }
//
//    public String getStringResponse() {
//        return strValue;
//    }
//
//}