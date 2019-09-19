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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.stats.StatsLogger;

public class PravegaBenchmarkDriver implements BenchmarkDriver {
    private ClientConfig config;
    private String scopeName;
    private StreamManager manager;

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        config = readConfig(configurationFile);
        scopeName = "examples";
        manager = StreamManager.create(config);
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static ClientConfig readConfig(File configurationFile) throws IOException {
        // TODO: Do not read into ClientConfig class.
        ClientConfig tempConfig = mapper.readValue(configurationFile, ClientConfig.class);
        return ClientConfig.builder()
                .controllerURI(tempConfig.getControllerURI())
                .build();
    }

    @Override
    public String getTopicNamePrefix() {
        return "pravega-benchmark";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
//        manager.createScope(scopeName);
        manager.createStream(scopeName, topic,
                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(partitions)).build());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        BenchmarkProducer producer = new PravegaBenchmarkProducer(topic, config, scopeName);
        return CompletableFuture.completedFuture(producer);
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        BenchmarkConsumer consumer = new PravegaBenchmarkConsumer(topic, subscriptionName, consumerCallback, config, scopeName);
        return CompletableFuture.completedFuture(consumer);
    }

    @Override
    public void close() throws Exception {
        manager.close();
    }
}
