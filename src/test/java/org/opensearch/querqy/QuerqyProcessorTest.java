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

package org.opensearch.querqy;

import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.opensearch.querqy.query.AbstractLuceneQueryTest.bq;
import static org.opensearch.querqy.query.AbstractLuceneQueryTest.dmq;
import static org.opensearch.querqy.query.AbstractLuceneQueryTest.tq;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.TermQuery;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.opensearch.common.collect.List;
import org.opensearch.querqy.query.AbstractLuceneQueryTest;
import querqy.lucene.LuceneQueries;

import java.util.Arrays;
import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class QuerqyProcessorTest {


    @Test
    public void testThatAppendFilterQueriesForNullFilterQueries() {
        final LuceneQueries queries = new LuceneQueries(
                new TermQuery(new Term("f1", "a")),
                null,
                Collections.singletonList(new TermQuery(new Term("f1", "boost"))),
                new TermQuery(new Term("f1", "a")), null, false, false);

        final BooleanQuery.Builder builder = new BooleanQuery.Builder();

        final QuerqyProcessor querqyProcessor = new QuerqyProcessor(mock(RewriterShardContexts.class), null);
        querqyProcessor.appendFilterQueries(queries, builder);
        final BooleanQuery booleanQuery = builder.build();
        assertThat(booleanQuery.clauses(), everyItem(CoreMatchers.not(AbstractLuceneQueryTest.anyFilter())));
        assertThat(booleanQuery.clauses(), everyItem(CoreMatchers.not(AbstractLuceneQueryTest.anyMustNot())));
        assertThat(booleanQuery.clauses(), everyItem(CoreMatchers.not(AbstractLuceneQueryTest.anyMust())));

    }

    @Test
    public void testThatAppendFilterQueriesForNoFilterQueries() {

        final LuceneQueries queries = new LuceneQueries(
                new TermQuery(new Term("f1", "a")),
                Collections.emptyList(),
                Collections.singletonList(new TermQuery(new Term("f1", "boost"))), new TermQuery(new Term("f1", "a")),
                null, false, false);

        final BooleanQuery.Builder builder = new BooleanQuery.Builder();

        final QuerqyProcessor querqyProcessor = new QuerqyProcessor(mock(RewriterShardContexts.class), null);
        querqyProcessor.appendFilterQueries(queries, builder);
        final BooleanQuery booleanQuery = builder.build();
        assertThat(booleanQuery.clauses(), everyItem(CoreMatchers.not(AbstractLuceneQueryTest.anyFilter())));
        assertThat(booleanQuery.clauses(), everyItem(CoreMatchers.not(AbstractLuceneQueryTest.anyMustNot())));
        assertThat(booleanQuery.clauses(), everyItem(CoreMatchers.not(AbstractLuceneQueryTest.anyMust())));

    }

    @Test
    public void testThatAllNegativeFilterQueryGetsAppended() {
        final DisjunctionMaxQuery dmqNeg = new DisjunctionMaxQuery(Arrays.asList(new TermQuery(new Term("f1", "filter_a")),
                new TermQuery(new Term("f2", "filter_a"))), 1f);

        final BooleanQuery.Builder container = new BooleanQuery.Builder();
        container.add(dmqNeg, BooleanClause.Occur.MUST_NOT);

        final LuceneQueries queries = new LuceneQueries(
                new TermQuery(new Term("f1", "u")),
                Collections.singletonList(container.build()),
                Collections.singletonList(new TermQuery(new Term("f1", "boost"))), new TermQuery(new Term("f1", "u")),
                null, false, false);

        final BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("f1", "u")), BooleanClause.Occur.MUST);


        final QuerqyProcessor querqyProcessor = new QuerqyProcessor(mock(RewriterShardContexts.class), null);
        querqyProcessor.appendFilterQueries(queries, builder);

        final BooleanQuery booleanQuery = builder.build();

        assertThat(booleanQuery,
                AbstractLuceneQueryTest.bq(
                        AbstractLuceneQueryTest.tq(BooleanClause.Occur.MUST, "f1", "u"),
                        AbstractLuceneQueryTest.dmq(
                                BooleanClause.Occur.MUST_NOT,
                                List.of(
                                        AbstractLuceneQueryTest.tq("f1", "filter_a"),
                                        AbstractLuceneQueryTest.tq("f2", "filter_a")
                                )
                        )
                )

        );

    }
}