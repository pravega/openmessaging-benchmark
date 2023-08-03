/**
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
package io.openmessaging.benchmark.driver.pravega;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * This class is used to customize json payload by updating attributes of json.
 */
public class CustomPayloadUtils {
    private static final Logger log = LoggerFactory.getLogger(CustomPayloadUtils.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * If isCustomPayload is true, customize the byte array payload by updating json attribute values
     * like Timestamp to make payload unique for almost each request.
     * If isCustomPayload is false no change in payload.
     *
     * @param isCustomPayload custom payload is enabled or not
     * @param payload static default payload to send to messaging system.
     * @return customized payload in byte array
     */
    public static byte[] customizePayload(boolean isCustomPayload, byte[] payload) {
        if(isCustomPayload && ArrayUtils.isNotEmpty(payload)) {
            try {
                Map<String, Object> jsonPayload = mapper.readValue(payload, Map.class);
                jsonPayload.put("Timestamp",System.currentTimeMillis());
                payload = mapper.writeValueAsBytes(jsonPayload);
                if(log.isDebugEnabled()) {
                    log.debug("customPayload: {}", new String(payload));
                }
            } catch (IOException e) {
                log.warn("exception occur while customising payload: {}", new String(payload), e);
            }
        }
        return payload;
    }
}
