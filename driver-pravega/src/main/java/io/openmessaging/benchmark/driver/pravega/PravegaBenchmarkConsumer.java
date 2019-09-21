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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PravegaBenchmarkConsumer implements BenchmarkConsumer {
    private static final Logger log = LoggerFactory.getLogger(PravegaBenchmarkDriver.class);

    private final ExecutorService executor;
    private final EventStreamReader<byte[]> reader;
    private volatile boolean closed = false;    // TODO: use atomic boolean?

    public PravegaBenchmarkConsumer(String topic, String scopeName, String subscriptionName, ConsumerCallback consumerCallback,
                                    EventStreamClientFactory clientFactory, ReaderGroupManager readerGroupManager) {
        log.info("PravegaBenchmarkConsumer: BEGIN: subscriptionName={}, topic={}", subscriptionName, topic);
        // Create reader group if it doesn't already exist.
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scopeName, topic))
                .build();
        readerGroupManager.createReaderGroup(subscriptionName, readerGroupConfig);
        // Create reader.
        reader = clientFactory.createReader(
                UUID.randomUUID().toString(),
                subscriptionName,
                new ByteArraySerializer(),
                ReaderConfig.builder().build());
        // Start a thread to read events.
        this.executor = Executors.newSingleThreadExecutor();
        this.executor.submit(() -> {
           while (!closed) {
               try {
                   EventRead<byte[]> record = reader.readNextEvent(1000);
                   if (record.getEvent() != null) {
                       // TODO: must get publish time from event
                       consumerCallback.messageReceived(record.getEvent(), System.currentTimeMillis());
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
        closed = true;
        this.executor.shutdown();
        this.executor.awaitTermination(1, TimeUnit.MINUTES);
        reader.close();
    }
}
