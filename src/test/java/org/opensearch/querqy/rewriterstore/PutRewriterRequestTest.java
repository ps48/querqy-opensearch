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

package org.opensearch.querqy.rewriterstore;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.opensearch.querqy.DummyOpenSearchRewriterFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PutRewriterRequestTest {

    @Test
    public void testValidateMissingRewriterId() {

        final PutRewriterRequest invalidRequest = new PutRewriterRequest(null, null);
        final ActionRequestValidationException validationResult = invalidRequest.validate();
        assertNotNull(validationResult);
    }

    @Test
    public void testValidateMissingClassConfig() {

        final PutRewriterRequest invalidRequest = new PutRewriterRequest("r8", Collections.emptyMap());
        final ActionRequestValidationException validationResult = invalidRequest.validate();
        assertNotNull(validationResult);

    }

    @Test
    public void testInvalidConfig() {

        final Map<String, Object> content = new HashMap<>();
        content.put("class", DummyOpenSearchRewriterFactory.class.getName());
        final Map<String, Object> config = new HashMap<>();
        config.put("error", "an error message");
        content.put("config", config);

        final PutRewriterRequest invalidRequest = new PutRewriterRequest("r8", content);
        final ActionRequestValidationException validationResult = invalidRequest.validate();
        assertNotNull(validationResult);
        assertThat(validationResult.validationErrors(), Matchers.contains("an error message"));

    }

    @Test
    public void testValidConfig() {

        final Map<String, Object> content = new HashMap<>();
        content.put("class", DummyOpenSearchRewriterFactory.class.getName());
        final Map<String, Object> config = new HashMap<>();
        content.put("config", config);

        final PutRewriterRequest validRequest = new PutRewriterRequest("r8", content);
        final ActionRequestValidationException validationResult = validRequest.validate();
        assertNull(validationResult);

    }

    @Test
    public void testStreamSerialization() throws IOException {

        final Map<String, Object> content = new HashMap<>();
        content.put("class", DummyOpenSearchRewriterFactory.class.getName());
        final Map<String, Object> config = new HashMap<>();
        config.put("prop1", "Some value");
        content.put("config", config);

        final PutRewriterRequest request1 = new PutRewriterRequest("r8", content);

        final BytesStreamOutput output = new BytesStreamOutput();
        request1.writeTo(output);
        output.flush();

        final PutRewriterRequest request2 = new PutRewriterRequest(output.bytes().streamInput());

        assertEquals(request1.getRewriterId(), request2.getRewriterId());
        assertEquals(request1.getContent(), request2.getContent());

    }

}