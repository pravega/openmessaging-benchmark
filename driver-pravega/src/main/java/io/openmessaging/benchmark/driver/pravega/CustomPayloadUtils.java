package io.openmessaging.benchmark.driver.pravega;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class CustomPayloadUtils {
    private static final Logger log = LoggerFactory.getLogger(CustomPayloadUtils.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static byte[] customizePayload(boolean isCustomPayload, byte[] payload) {
        if(isCustomPayload && payload!=null && payload.length > 0) {
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
