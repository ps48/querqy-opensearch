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

package org.opensearch.querqy.infologging;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class LogMessageTest {

    @Test
    public void appendValueTest() {
        final Map<String, Object> obj = new HashMap<>();
        obj.put("str", "string");
        obj.put("integer", 11);
        obj.put("float", 2.81);
        obj.put("long", 10001L);
        obj.put("bool", Boolean.TRUE);
        obj.put("nil", null);
        obj.put("empty", new HashMap<>());
        final Map<String, Object> obj2 = new HashMap<>();
        obj2.put("msg", "nothing");
        obj2.put("list", Arrays.asList(new HashMap<>(), 15, "end"));
        obj.put("an_object", obj2);

        final StringBuilder sb = new StringBuilder();
        LogMessage.appendValue(obj, sb);
        assertEquals("{\"str\":\"string\",\"nil\":null,\"bool\":true,\"integer\":11,\"float\":2.81,\"long\":10001," +
                "\"an_object\":{\"msg\":\"nothing\",\"list\":[{},15,\"end\"]},\"empty\":{}}", sb.toString());

    }

}