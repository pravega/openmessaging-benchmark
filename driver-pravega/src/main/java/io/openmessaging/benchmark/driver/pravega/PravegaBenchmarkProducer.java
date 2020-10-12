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

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.pravega.schema.common.EventTimeStampAware;
import io.openmessaging.benchmark.driver.pravega.schema.generated.avro.User;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class PravegaBenchmarkProducer implements BenchmarkProducer {
    private static final Logger log = LoggerFactory.getLogger(PravegaBenchmarkProducer.class);

    private final EventStreamWriter writer;
    private final Serializer serializer;
    private final boolean includeTimestampInEvent;
    private ByteBuffer timestampAndPayload;
    private EventTimeStampAware eventTimeStampAwarePayload;

    public PravegaBenchmarkProducer(String streamName, EventStreamClientFactory clientFactory,
                                    boolean includeTimestampInEvent,
                                    boolean enableConnectionPooling, Serializer serializer) {
        log.info("PravegaBenchmarkProducer: BEGIN: streamName={}", streamName);
        this.serializer = serializer;
        writer = clientFactory.createEventWriter(
                streamName,
                serializer,
                EventWriterConfig.builder()
                        .enableConnectionPooling(enableConnectionPooling)
                        .build());
        this.includeTimestampInEvent = includeTimestampInEvent;
    }

    public void setPayload(EventTimeStampAware eventTimeStampAwarePayload) {
        this.eventTimeStampAwarePayload = eventTimeStampAwarePayload;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        if (includeTimestampInEvent) {
            return writeEvent(key, includeTimestampInPayload(payload));
        }
        return writeEvent(key, ByteBuffer.wrap(payload));
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key) {
        if (includeTimestampInEvent) {
            eventTimeStampAwarePayload.setEventTimestamp(System.currentTimeMillis());
        }
        return writeObjectEvent(key, eventTimeStampAwarePayload);
    }

    private ByteBuffer includeTimestampInPayload(byte[] payload) {
        if (timestampAndPayload == null || timestampAndPayload.limit() != Long.BYTES + payload.length) {
            timestampAndPayload = ByteBuffer.allocate(Long.BYTES + payload.length);
        } else {
            timestampAndPayload.position(0);
        }
        timestampAndPayload.putLong(System.currentTimeMillis()).put(payload).flip();
        return timestampAndPayload;
    }

    private CompletableFuture<Void> writeObjectEvent(Optional<String> key, EventTimeStampAware eventTimeStampAwarePayload) {
        if (key.isPresent()) {
            return writer.writeEvent(key.get(), eventTimeStampAwarePayload);
        } else {
            return writer.writeEvent(eventTimeStampAwarePayload);
        }
    }

    private CompletableFuture<Void> writeEvent(Optional<String> key, ByteBuffer payload) {
        return (key.isPresent()) ? writer.writeEvent(key.get(), payload) : writer.writeEvent(payload);
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }
}
