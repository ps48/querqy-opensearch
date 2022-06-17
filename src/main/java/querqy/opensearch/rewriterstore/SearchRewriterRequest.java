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

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestRequest;
import querqy.opensearch.QuerqyPlugin;
import querqy.opensearch.query.QuerqyQueryBuilder;

import java.io.IOException;

public class SearchRewriterRequest extends ActionRequest {

    private QuerqyQueryBuilder querqyQueryBuilder = null;
    private String searchParams = "";
    private String ExceptionMessage = "";

    public SearchRewriterRequest(final StreamInput in) throws IOException {
        super(in);
        searchParams = in.readString();
        querqyQueryBuilder = new QuerqyQueryBuilder(in, QuerqyPlugin.getQueryProcessor());
    }

    public SearchRewriterRequest(final String searchParams, final RestRequest request) {
        super();
        this.searchParams = searchParams;
        try {
            this.querqyQueryBuilder = QuerqyQueryBuilder.fromXContent(XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, request.content(), XContentType.JSON),
                    QuerqyPlugin.getQueryProcessor());
        }
        catch (Exception e) {
            ExceptionMessage = e.getMessage();
        }

    }

    @Override
    @SuppressWarnings("unchecked")
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (this.searchParams == null){
            validationException = ValidateActions.addValidationError("Invalid index name", validationException);
        }
        if (this.querqyQueryBuilder == null || !this.ExceptionMessage.equals("")){
            validationException = ValidateActions.addValidationError("QuerqyQueryBuilder failed: "+ExceptionMessage, validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(searchParams);
        querqyQueryBuilder.doWriteTo(out);
    }

    public String getSearchParams() {
        return searchParams;
    }

    public QuerqyQueryBuilder getQuerqyQueryBuilder() {
        return querqyQueryBuilder;
    }
}
