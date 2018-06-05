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
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.stream.impl.JavaSerializer;
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PravegaBenchmarkCounsumer implements BenchmarkConsumer {

    private final ExecutorService executor;
    private EventStreamReader reader;
    private boolean closed = false;

    public PravegaBenchmarkCounsumer(String topic, String subscriptionName, ConsumerCallback consumerCallback, ClientConfig config) {
        ReaderGroupManager.withScope("benchmark", config)
                          .createReaderGroup(subscriptionName, ReaderGroupConfig.builder()
                                                                                .stream(Stream.of("benchmark/" + topic))
                                                                                .build());
        reader = ClientFactory.withScope("benchmark", config)
                              .createReader(UUID.randomUUID().toString(), subscriptionName,
                                      new ByteArraySerializer(), ReaderConfig.builder().build());
        this.executor = Executors.newSingleThreadExecutor();
        this.executor.submit(() -> {
           while (!closed) {
               try {
                   EventRead<byte[]> record;
                   if ((record = reader.readNextEvent(1000))!= null) {
                       consumerCallback.messageReceived(record.getEvent(), System.currentTimeMillis());
                   }
               } catch (ReinitializationRequiredException e) {
                   reader = ClientFactory.withScope("benchmark", config)
                                         .createReader(UUID.randomUUID().toString(), subscriptionName,
                                                 new ByteArraySerializer(), ReaderConfig.builder().build());
               }
           }
        });
    }

    @Override
    public void close() throws Exception {
        closed = true;
        reader.close();
        this.executor.shutdown();
        this.executor.awaitTermination(1, TimeUnit.MINUTES);
    }
}
