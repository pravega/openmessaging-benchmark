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
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.stream.impl.JavaSerializer;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class PravegaBenchmarkProducer implements BenchmarkProducer {

    private final EventStreamWriter producer;

    public PravegaBenchmarkProducer(EventStreamWriter pravegaProducer) {
        this.producer = pravegaProducer;
    }

    public PravegaBenchmarkProducer(String streamName, ClientConfig config, String scopeName) {
        this(ClientFactory.withScope(scopeName, config)
        .createEventWriter(streamName, new ByteArraySerializer(), EventWriterConfig.builder().build()));

    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        if (!key.isPresent()) {
            key = Optional.of(UUID.randomUUID().toString());
        }
        return producer.writeEvent(key.get(), payload);
    }

    @Override
    public void close() throws Exception {
        producer.close();

    }
}
