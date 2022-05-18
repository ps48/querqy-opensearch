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

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.querqy.query.QuerqyQueryBuilder;
import org.opensearch.querqy.rewriterstore.*;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;
import org.opensearch.querqy.infologging.Log4jSink;
import org.opensearch.querqy.rewriterstore.DeleteRewriterAction;
import org.opensearch.querqy.rewriterstore.NodesClearRewriterCacheAction;
import org.opensearch.querqy.rewriterstore.NodesReloadRewriterAction;
import org.opensearch.querqy.rewriterstore.RestDeleteRewriterAction;
import org.opensearch.querqy.rewriterstore.RestPutRewriterAction;
import org.opensearch.querqy.rewriterstore.PutRewriterAction;
import org.opensearch.querqy.rewriterstore.TransportDeleteRewriterAction;
import org.opensearch.querqy.rewriterstore.TransportNodesClearRewriterCacheAction;
import org.opensearch.querqy.rewriterstore.TransportNodesReloadRewriterAction;
import org.opensearch.querqy.rewriterstore.TransportPutRewriterAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class QuerqyPlugin extends Plugin implements SearchPlugin, ActionPlugin {


    private final QuerqyProcessor querqyProcessor;
    private final RewriterShardContexts rewriterShardContexts;

    public QuerqyPlugin(final Settings settings) {
        rewriterShardContexts = new RewriterShardContexts(settings);
        querqyProcessor = new QuerqyProcessor(rewriterShardContexts, new Log4jSink());
    }

    @Override
    public void onIndexModule(final IndexModule indexModule) {

        indexModule.addIndexEventListener(rewriterShardContexts);

    }

    /**
     * The new {@link QuerySpec}s defined by this plugin.
     */
    @Override
    public List<QuerySpec<?>> getQueries() {
        return Collections.singletonList(
                new QuerySpec<QueryBuilder>(
                        QuerqyQueryBuilder.NAME,
                        (in) -> new QuerqyQueryBuilder(in, querqyProcessor),
                        (parser) -> QuerqyQueryBuilder.fromXContent(parser, querqyProcessor)));
    }

    @Override
    public List<RestHandler> getRestHandlers(final Settings settings, final RestController restController,
                                             final ClusterSettings clusterSettings,
                                             final IndexScopedSettings indexScopedSettings,
                                             final SettingsFilter settingsFilter,
                                             final IndexNameExpressionResolver indexNameExpressionResolver,
                                             final Supplier<DiscoveryNodes> nodesInCluster) {

        return Arrays.asList(new RestPutRewriterAction(), new RestDeleteRewriterAction());

    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return unmodifiableList(asList(
                new ActionHandler<>(PutRewriterAction.INSTANCE, TransportPutRewriterAction.class),
                new ActionHandler<>(NodesReloadRewriterAction.INSTANCE, TransportNodesReloadRewriterAction.class),
                new ActionHandler<>(DeleteRewriterAction.INSTANCE, TransportDeleteRewriterAction.class),
                new ActionHandler<>(NodesClearRewriterCacheAction.INSTANCE, TransportNodesClearRewriterCacheAction
                        .class)

        ));
    }

    @Override
    public Collection<Object> createComponents(final Client client, final ClusterService clusterService,
                                               final ThreadPool threadPool,
                                               final ResourceWatcherService resourceWatcherService,
                                               final ScriptService scriptService,
                                               final NamedXContentRegistry xContentRegistry,
                                               final Environment environment, final NodeEnvironment nodeEnvironment,
                                               final NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver indexNameExpressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        return Arrays.asList(rewriterShardContexts, querqyProcessor);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(Setting.intSetting(Constants.SETTINGS_QUERQY_INDEX_NUM_REPLICAS, 1, 0,
                Setting.Property.NodeScope));

    }
}
