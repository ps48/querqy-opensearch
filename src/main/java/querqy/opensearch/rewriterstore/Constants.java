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

public interface Constants {

    // TODO: configurable?

    String QUERQY_INDEX_NAME = ".opensearch-querqy";

    String QUERQY_REWRITER_BASE_ROUTE = "/_plugins/_querqy/rewriter";

    String SETTINGS_QUERQY_INDEX_NUM_REPLICAS = "querqy.store.replicas";

    int DEFAULT_QUERQY_INDEX_NUM_REPLICAS = 1;
}
