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

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.pravega.testobj.User;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PravegaBenchmarkConsumer implements BenchmarkConsumer {
    private static final Logger log = LoggerFactory.getLogger(PravegaBenchmarkConsumer.class);

    private final ExecutorService executor;
    private final EventStreamReader reader;
    private final AtomicBoolean closed = new AtomicBoolean(false);



    public PravegaBenchmarkConsumer(String streamName, String scopeName, String subscriptionName, ConsumerCallback consumerCallback,
                                    EventStreamClientFactory clientFactory, ReaderGroupManager readerGroupManager,
                                    boolean includeTimestampInEvent) {
        log.info("PravegaBenchmarkConsumer: BEGIN: subscriptionName={}, streamName={}", subscriptionName, streamName);
        // Create reader group if it doesn't already exist.
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scopeName, streamName))
                .build();
        readerGroupManager.createReaderGroup(subscriptionName, readerGroupConfig);
        // Create reader.
        reader = clientFactory.createReader(
                UUID.randomUUID().toString(),
                subscriptionName,
                new ByteBufferSerializer(),
                ReaderConfig.builder().disableTimeWindows(true).build());
        // Start a thread to read events.
        this.executor = Executors.newSingleThreadExecutor();
        this.executor.submit(() -> {
           while (!closed.get()) {
               try {
                   final ByteBuffer event = (ByteBuffer) reader.readNextEvent(1000).getEvent();
                   if (event != null) {
                       long eventTimestamp;
                       if (includeTimestampInEvent) {
                           eventTimestamp = event.getLong();
                       } else {
                           // This will result in an invalid end-to-end latency measurement of 0 seconds.
                           eventTimestamp = TimeUnit.MICROSECONDS.toMillis(Long.MAX_VALUE);
                       }
                       byte[] payload = new byte[event.remaining()];
                       event.get(payload);
                       consumerCallback.messageReceived(payload, eventTimestamp);
                   }
               } catch (ReinitializationRequiredException e) {
                   log.error("Exception during read", e);
                   throw e;
               }
           }
        });
    }

    public PravegaBenchmarkConsumer(String streamName, String scopeName, String subscriptionName, ConsumerCallback consumerCallback,
                                    EventStreamClientFactory clientFactory, ReaderGroupManager readerGroupManager,
                                    boolean includeTimestampInEvent, Serializer<Object> deserializer) {
        log.info("PravegaBenchmarkConsumer: BEGIN: subscriptionName={}, streamName={}", subscriptionName, streamName);
        // Create reader group if it doesn't already exist.
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scopeName, streamName))
                .build();
        readerGroupManager.createReaderGroup(subscriptionName, readerGroupConfig);

        reader = clientFactory.createReader(
                UUID.randomUUID().toString(),
                subscriptionName,
                deserializer,
                ReaderConfig.builder().disableTimeWindows(true).build());

        // Start a thread to read events.
        this.executor = Executors.newSingleThreadExecutor();
        this.executor.submit(() -> {
            while (!closed.get()) {
                try {
                    final User event = (User) reader.readNextEvent(1000).getEvent();
                    if (event != null) {
                        long eventTimestamp;
                        if (includeTimestampInEvent) { // todo
                            eventTimestamp = event.getEventTimestamp(); // event.getEventTimestamp();
                        } else {
                            // This will result in an invalid end-to-end latency measurement of 0 seconds.
                            eventTimestamp = TimeUnit.MICROSECONDS.toMillis(Long.MAX_VALUE);
                        }
                        // todo use consumerCallback.messageReceived with payload as byte[]
                        consumerCallback.eventReceived(eventTimestamp);
                    }
                } catch (ReinitializationRequiredException e) {
                    log.error("Exception during read", e);
                    throw e;
                }
            }
        });


    }

    @Override
    public void close() throws Exception {
        closed.set(true);
        this.executor.shutdown();
        this.executor.awaitTermination(1, TimeUnit.MINUTES);
        reader.close();
    }
}
