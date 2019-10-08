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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class PravegaBenchmarkProducer implements BenchmarkProducer {
    private static final Logger log = LoggerFactory.getLogger(PravegaBenchmarkProducer.class);

    private final EventStreamWriter<ByteBuffer> writer;
    private final boolean includeTimestampInEvent;

    public PravegaBenchmarkProducer(String streamName, EventStreamClientFactory clientFactory,
                                    boolean includeTimestampInEvent,
                                    boolean enableConnectionPooling) {
        log.info("PravegaBenchmarkProducer: BEGIN: streamName={}", streamName);
        writer = clientFactory.createEventWriter(
                streamName,
                new ByteBufferSerializer(),
                EventWriterConfig.builder()
                        .enableConnectionPooling(enableConnectionPooling)
                        .build());
        this.includeTimestampInEvent = includeTimestampInEvent;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        ByteBuffer payloadToWrite;
        if (includeTimestampInEvent) {
            // We must create a new buffer for the combined event timestamp and payload.
            // This requires copying the entire payload.
            long eventTimestamp = System.currentTimeMillis();
            payloadToWrite = ByteBuffer.allocate(Long.BYTES + payload.length);
            payloadToWrite.putLong(eventTimestamp);
            payloadToWrite.put(payload);
            payloadToWrite.flip();
        } else {
            payloadToWrite = ByteBuffer.wrap(payload);
        }
        if (key.isPresent()) {
            return writer.writeEvent(key.get(), payloadToWrite);
        } else {
            return writer.writeEvent(payloadToWrite);
        }
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }
}
