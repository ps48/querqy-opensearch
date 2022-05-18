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

package org.opensearch.querqy.rewriter;

import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.querqy.QuerqyProcessor;
import org.opensearch.querqy.query.QuerqyQueryBuilder;
import org.opensearch.querqy.rewriterstore.PutRewriterAction;
import org.opensearch.search.SearchHits;
import org.opensearch.querqy.query.MatchingQuery;
import org.opensearch.querqy.query.Rewriter;
import org.opensearch.querqy.rewriterstore.PutRewriterRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonList;

public class ReplaceRewriterIntegrationTest extends AbstractRewriterIntegrationTest {

    public void testReplaceRewriterRules() throws ExecutionException, InterruptedException {
        indexDocs(
                doc("id", "1", "field1", "test1"),
                doc("id", "2", "field1", "test2"),
                doc("id", "3", "field1", "test1")
        );

        final Map<String, Object> content = new HashMap<>();
        content.put("class", "querqy.opensearch.rewriter.ReplaceRewriterFactory");

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "TEST => TEST1 \n TEST2 => TEST");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("replace_rules", content);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("replace_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("test"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        SearchRequestBuilder searchRequestBuilder = client().prepareSearch(getIndexName());
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        SearchHits hits = response.getHits();

        assertEquals(2L, hits.getTotalHits().value);

        querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("replace_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("test2"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        searchRequestBuilder = client().prepareSearch(getIndexName());
        searchRequestBuilder.setQuery(querqyQuery);

        response = client().search(searchRequestBuilder.request()).get();
        hits = response.getHits();

        assertEquals(0L, hits.getTotalHits().value);

    }

}
