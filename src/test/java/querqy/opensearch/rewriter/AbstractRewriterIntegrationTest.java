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

package querqy.opensearch.rewriter;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import querqy.opensearch.QuerqyPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singleton;

public abstract class AbstractRewriterIntegrationTest extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return singleton(QuerqyPlugin.class);
    }

    private static final String INDEX_NAME = "test_index";

    protected static String getIndexName() {
        return INDEX_NAME;
    }

    public IndexDocument doc(Object... kv) {
        if (kv.length % 2 != 0) {
            throw new RuntimeException("Input size must be even");
        }

        final Map<String, Object> doc = new HashMap<>();
        for (int i = 0; i < kv.length; i = i + 2) {
            doc.put((String) kv[i], kv[i + 1]);
        }

        return IndexDocument.of(doc);
    }

    public final void indexDocs(final IndexDocument... docs) {
        client().admin().indices().prepareCreate(getIndexName()).get();

        Arrays.stream(docs).forEach(doc ->
                client().prepareIndex(getIndexName())
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .setSource(doc.getDoc())
                        .get());
    }

    public static class IndexDocument {
        final Map<String, Object> doc;

        private IndexDocument(final Map<String, Object> doc) {
            this.doc = doc;
        }

        public Map<String, Object> getDoc() {
            return doc;
        }

        public static IndexDocument of(final Map<String, Object> doc) {
            return new IndexDocument(doc);
        }
    }
}
