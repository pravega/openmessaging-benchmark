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
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.pravega.config.PravegaConfig;
import io.openmessaging.benchmark.driver.pravega.config.SchemaRegistryConfig;
import io.openmessaging.benchmark.driver.pravega.schema.common.EventTimeStampAware;
import io.openmessaging.benchmark.driver.pravega.schema.common.UnsupportedSerializationFormatException;
import io.openmessaging.benchmark.driver.pravega.schema.generated.avro.User;
import io.openmessaging.benchmark.driver.pravega.schema.json.JSONUser;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import lombok.Getter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class PravegaBenchmarkDriver implements BenchmarkDriver {
    private static final Logger log = LoggerFactory.getLogger(PravegaBenchmarkDriver.class);

    private static final ObjectWriter objectWriter = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private PravegaConfig config;
    private ClientConfig clientConfig;
    private String scopeName;
    private StreamManager streamManager;
    private ReaderGroupManager readerGroupManager;
    private EventStreamClientFactory clientFactory;
    private final List<String> createdTopics = new ArrayList<>();
    // for Schema-registry support
    @Getter
    private Serializer serializer, deserializer;
    @Getter
    private boolean schemaRegistryEnabled;
    @Getter
    private SerializationFormat serializationFormat;
    @Getter
    private File schemaFile;
    @Getter
    private boolean timestampIncludedInEvent;

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException, URISyntaxException {
        config = readConfig(configurationFile);
        log.info("Pravega driver configuration: {}", objectWriter.writeValueAsString(config));
        if (config.enableSchemaRegistry) {
            Assert.assertNotNull("If enableSchemaRegistry is True, schemaRegistry config is required.", config.schemaRegistry);
        }
        clientConfig = ClientConfig.builder().controllerURI(URI.create(config.client.controllerURI)).build();
        scopeName = config.client.scopeName;
        streamManager = StreamManager.create(clientConfig);
        readerGroupManager = ReaderGroupManager.withScope(scopeName, clientConfig);
        clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig);

        this.timestampIncludedInEvent = config.includeTimestampInEvent;

        if (config.enableSchemaRegistry) {
            SchemaRegistryConfig schemaRegistryConfig = config.schemaRegistry;
            log.info("schemaRegistryConfig: {}", schemaRegistryConfig);
            this.schemaRegistryEnabled = true;
            this.serializationFormat = schemaRegistryConfig.serializationFormat;
            Assert.assertFalse("Do not yet support both transactions with schema registry", config.enableTransaction);
            Assert.assertNotNull("If schema-registry support is enabled, need to specify supported serializationFormat: Avro, Json or Protobuf", this.serializationFormat);

            SchemaRegistryClientConfig config;
            // create serializer and deserializer
            try {
                config = SchemaRegistryClientConfig.builder()
                        .schemaRegistryUri(new URI(schemaRegistryConfig.schemaRegistryURI)).build();
            } catch (URISyntaxException e) {
                log.error("schemaRegistryURI {} is invalid.", schemaRegistryConfig.schemaRegistryURI, e);
                throw e;
            }

            if (this.serializationFormat == SerializationFormat.Avro) {
                SerializerConfig serializerConfig = SerializerConfig.builder()
                        .groupId(schemaRegistryConfig.groupId).registryConfig(config)
                        .createGroup(SerializationFormat.Avro).registerSchema(true)
                        .build();
                AvroSchema<User> schema = AvroSchema.of(User.class);
                this.serializer = SerializerFactory.avroSerializer(serializerConfig, schema);
                this.deserializer = SerializerFactory.avroDeserializer(serializerConfig, schema);

                String schemaFile = schemaRegistryConfig.schemaFile;
                Assert.assertTrue(String.format("Schema file %s is not readable", schemaFile), Files.isReadable(Paths.get(schemaFile)));
                this.schemaFile = new File(schemaFile);

            } else if (this.serializationFormat == SerializationFormat.Json) {
                SerializerConfig serializerConfig = SerializerConfig.builder()
                        .groupId(schemaRegistryConfig.groupId).registryConfig(config)
                        .createGroup(SerializationFormat.Json, Compatibility.allowAny(), true).registerSchema(true)
                        .build();
                JSONSchema<JSONUser> schema = JSONSchema.of(JSONUser.class);
                this.serializer = SerializerFactory.jsonSerializer(serializerConfig, schema);
                this.deserializer = SerializerFactory.jsonDeserializer(serializerConfig, schema);
            } else {
                throw new UnsupportedSerializationFormatException(serializationFormat);
            }
        } else {
            this.serializer = new ByteBufferSerializer();
            this.deserializer = new ByteBufferSerializer();
        }
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static PravegaConfig readConfig(File configurationFile) throws IOException {
        return mapper.readValue(configurationFile, PravegaConfig.class);
    }

    /**
     * Clean Pravega stream name to only allow alpha-numeric and "-".
     */
    private String cleanName(String name) {
        return name.replaceAll("[^A-Za-z0-9-]", "");
    }

    @Override
    public String getTopicNamePrefix() {
        return "openmessaging-benchmark";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        topic = cleanName(topic);
        log.info("createTopic: topic={}, partitions={}", topic, partitions);
        synchronized (createdTopics) {
            createdTopics.add(topic);
        }
        if (config.createScope) {
            streamManager.createScope(scopeName);
        }
        ScalingPolicy scalingPolicy;
        // Create a fixed or auto-scaling Stream based on user configuration.
        if (config.enableStreamAutoScaling && (config.eventsPerSecond != PravegaConfig.DEFAULT_STREAM_AUTOSCALING_VALUE ||
                config.kbytesPerSecond != PravegaConfig.DEFAULT_STREAM_AUTOSCALING_VALUE)) {
            scalingPolicy = config.eventsPerSecond != PravegaConfig.DEFAULT_STREAM_AUTOSCALING_VALUE ?
                    ScalingPolicy.byEventRate(config.eventsPerSecond, 2, partitions) :
                    ScalingPolicy.byDataRate(config.kbytesPerSecond, 2, partitions);
        } else {
            scalingPolicy = ScalingPolicy.fixed(partitions);
        }
        streamManager.createStream(scopeName, topic, StreamConfiguration.builder().scalingPolicy(scalingPolicy).build());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        topic = cleanName(topic);
        BenchmarkProducer producer;
        if (config.enableTransaction) { // todo schema
            producer = new PravegaBenchmarkTransactionProducer(topic, clientFactory, config.includeTimestampInEvent,
                    config.writer.enableConnectionPooling, config.eventsPerTransaction);
        } else {
            producer = new PravegaBenchmarkProducer(topic, clientFactory, config.includeTimestampInEvent,
                        config.writer.enableConnectionPooling, serializer);

        }
        return CompletableFuture.completedFuture(producer);
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(String topic, String subscriptionName,
            ConsumerCallback consumerCallback) {
        topic = cleanName(topic);
        subscriptionName = cleanName(subscriptionName);
        BenchmarkConsumer consumer;
        if (config.enableSchemaRegistry) {
            consumer = new PravegaBenchmarkConsumer(topic, scopeName, subscriptionName, consumerCallback,
                    clientFactory, readerGroupManager, config.includeTimestampInEvent, deserializer);
        } else {
            consumer = new PravegaBenchmarkConsumer(topic, scopeName, subscriptionName, consumerCallback,
                    clientFactory, readerGroupManager, config.includeTimestampInEvent);
        }
        return CompletableFuture.completedFuture(consumer);
    }

    private void deleteTopics() {
        synchronized (createdTopics) {
            for (String topic : createdTopics) {
                log.info("deleteTopics: topic={}", topic);
                streamManager.sealStream(scopeName, topic);
                if (config.deleteStreams) {
                    streamManager.deleteStream(scopeName, topic);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        log.info("close: clientConfig={}", clientConfig);
        if (clientFactory != null) {
            clientFactory.close();
        }
        if (readerGroupManager != null) {
            readerGroupManager.close();
        }
        if (streamManager != null) {
            deleteTopics();
            streamManager.close();
        }
    }
}
