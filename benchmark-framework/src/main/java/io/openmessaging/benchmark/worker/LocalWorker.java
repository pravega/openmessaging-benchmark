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
package io.openmessaging.benchmark.worker;

import static java.util.stream.Collectors.toList;

import com.google.common.primitives.Longs;
import com.google.protobuf.util.JsonFormat;
import io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver;
import io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver;
import io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkProducer;
import io.openmessaging.benchmark.driver.pravega.schema.common.EventTimeStampAware;
import io.openmessaging.benchmark.driver.pravega.schema.common.InvalidPayloadSerializedSizeException;
import io.openmessaging.benchmark.driver.pravega.schema.common.UnsupportedSerializationFormatException;
import io.openmessaging.benchmark.driver.pravega.schema.generated.avro.User;
import io.openmessaging.benchmark.driver.pravega.schema.generated.protobuf.Protobuf;
import io.openmessaging.benchmark.driver.pravega.schema.json.JSONUser;
import io.openmessaging.benchmark.utils.RandomGenerator;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.openmessaging.benchmark.utils.payload.FilePayloadReader;
import io.openmessaging.benchmark.utils.payload.PayloadReader;
import io.openmessaging.benchmark.worker.commands.*;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import org.HdrHistogram.Recorder;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.core.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;

import io.openmessaging.benchmark.DriverConfiguration;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.distributor.KeyDistributor;

public class LocalWorker implements Worker, ConsumerCallback {

    private BenchmarkDriver benchmarkDriver = null;

    private List<BenchmarkProducer> producers = new ArrayList<>();
    private List<BenchmarkConsumer> consumers = new ArrayList<>();

    private final RateLimiter rateLimiter = RateLimiter.create(1.0);
    private boolean rateLimiterEnabled = false;

    private final ExecutorService executor = new ForkJoinPool();

    // stats

    private final StatsLogger statsLogger;

    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder bytesSent = new LongAdder();
    private final Counter messagesSentCounter;
    private final Counter bytesSentCounter;

    private final LongAdder messagesReceived = new LongAdder();
    private final LongAdder bytesReceived = new LongAdder();
    private final Counter messagesReceivedCounter;
    private final Counter bytesReceivedCounter;

    private final LongAdder totalMessagesSent = new LongAdder();
    private final LongAdder totalMessagesReceived = new LongAdder();

    private final long publishLatencyMax = TimeUnit.SECONDS.toMicros(60);
    private final Recorder publishLatencyRecorder = new Recorder(publishLatencyMax, 5);
    private final Recorder cumulativePublishLatencyRecorder = new Recorder(publishLatencyMax, 5);
    private final OpStatsLogger publishLatencyStats;

    private final long endToEndLatencyMax = TimeUnit.HOURS.toMicros(12);
    private final Recorder endToEndLatencyRecorder = new Recorder(endToEndLatencyMax, 5);
    private final Recorder endToEndCumulativeLatencyRecorder = new Recorder(endToEndLatencyMax, 5);
    private final OpStatsLogger endToEndLatencyStats;

    private int payloadSize = 0;

    private boolean testCompleted = false;

    private boolean consumersArePaused = false;
    // Fields for Schema-registry support
    private SerializationFormat serializationFormat;
    private boolean need2ProbeProducers = true;
    private File schemaFile;
    private Serializer serializer;
    private boolean timestampIncludedInEvent;

    public LocalWorker() {
        this(NullStatsLogger.INSTANCE);
    }

    public LocalWorker(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;

        StatsLogger producerStatsLogger = statsLogger.scope("producer");
        this.messagesSentCounter = producerStatsLogger.getCounter("messages_sent");
        this.bytesSentCounter = producerStatsLogger.getCounter("bytes_sent");
        this.publishLatencyStats = producerStatsLogger.getOpStatsLogger("produce_latency");

        StatsLogger consumerStatsLogger = statsLogger.scope("consumer");
        this.messagesReceivedCounter = consumerStatsLogger.getCounter("messages_recv");
        this.bytesReceivedCounter = consumerStatsLogger.getCounter("bytes_recv");
        this.endToEndLatencyStats = consumerStatsLogger.getOpStatsLogger("e2e_latency");
    }

    @Override
    public void initializeDriver(File driverConfigFile) throws IOException {
        Preconditions.checkArgument(benchmarkDriver == null);
        testCompleted = false;

        DriverConfiguration driverConfiguration = mapper.readValue(driverConfigFile, DriverConfiguration.class);

        log.info("Driver: {}", writer.writeValueAsString(driverConfiguration));

        try {
            benchmarkDriver = (BenchmarkDriver) Class.forName(driverConfiguration.driverClass).newInstance();
            benchmarkDriver.initialize(driverConfigFile, statsLogger);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
        if (benchmarkDriver instanceof KafkaBenchmarkDriver) {
            this.need2ProbeProducers = true;
        } else if ((benchmarkDriver instanceof PravegaBenchmarkDriver) && (((PravegaBenchmarkDriver) benchmarkDriver).isSchemaRegistryEnabled())) {
            PravegaBenchmarkDriver pravegaBenchmarkDriver = ((PravegaBenchmarkDriver) benchmarkDriver);
            this.serializationFormat = pravegaBenchmarkDriver.getSerializationFormat();
            Assert.requireNonEmpty(this.serializationFormat, "Need serialization format defined in PravegaBenchmarkDriver when schema-registry support is enabled");
            this.schemaFile = pravegaBenchmarkDriver.getSchemaFile();
            this.need2ProbeProducers = false;
            this.serializer = pravegaBenchmarkDriver.getSerializer();
            this.timestampIncludedInEvent = pravegaBenchmarkDriver.isTimestampIncludedInEvent();
        } else {
            this.serializer = new ByteBufferSerializer();
        }

    }

    @Override
    public List<String> createTopics(TopicsInfo topicsInfo) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        Timer timer = new Timer();

        String topicPrefix = benchmarkDriver.getTopicNamePrefix();

        List<String> topics = new ArrayList<>();
        for (int i = 0; i < topicsInfo.numberOfTopics; i++) {
            String topic = String.format("%s-%s-%04d", topicPrefix, RandomGenerator.getRandomString(), i);
            topics.add(topic);
            futures.add(benchmarkDriver.createTopic(topic, topicsInfo.numberOfPartitionsPerTopic));
        }

        futures.forEach(CompletableFuture::join);

        log.info("Created {} topics in {} ms", topics.size(), timer.elapsedMillis());
        return topics;
    }

    @Override
    public void createProducers(List<String> topics) {
        Timer timer = new Timer();

        List<CompletableFuture<BenchmarkProducer>> futures = topics.stream()
                .map(topic -> benchmarkDriver.createProducer(topic)).collect(toList());

        futures.forEach(f -> producers.add(f.join()));
        log.info("Created {} producers in {} ms", producers.size(), timer.elapsedMillis());
    }

    @Override
    public void createConsumers(ConsumerAssignment consumerAssignment) {
        Timer timer = new Timer();

        List<CompletableFuture<BenchmarkConsumer>> futures = consumerAssignment.topicsSubscriptions.stream()
                .map(ts -> benchmarkDriver.createConsumer(ts.topic, ts.subscription, this)).collect(toList());

        futures.forEach(f -> consumers.add(f.join()));
        log.info("Created {} consumers in {} ms", consumers.size(), timer.elapsedMillis());
    }

    @Override
    public void startLoad(final ProducerWorkAssignment producerWorkAssignment) throws IOException {
        if (producers.size() == 0)
            return;
        int processors = Runtime.getRuntime().availableProcessors();
        int producersPerProcessor = (producers.size() + processors - 1) / processors;
        log.info("producers={}, availableProcessors={}, producersPerProcessor={}", producers.size(), processors, producersPerProcessor);

        final Function<BenchmarkProducer, KeyDistributor> assignKeyDistributor = (any) -> KeyDistributor
                .build(producerWorkAssignment.keyDistributorType);

        rateLimiterEnabled = producerWorkAssignment.publishRate != Double.POSITIVE_INFINITY;
        if (rateLimiterEnabled) {
            rateLimiter.setRate(producerWorkAssignment.publishRate);
        }
        log.info("rateLimiterEnabled={}, publishRate={}", rateLimiterEnabled, producerWorkAssignment.publishRate);

        if (this.serializationFormat != null) {
            log.info("Starting load with schema-registry support, serialization format {}", this.serializationFormat);
            EventTimeStampAware user;

            if (serializationFormat == SerializationFormat.Avro) {
                Schema schema = new Schema.Parser().parse(this.schemaFile);
                String payloadJSON = FileUtils.readFileToString(new File(producerWorkAssignment.payloadFile), "UTF-8");
                JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, payloadJSON);
                SpecificDatumReader<User> reader = new SpecificDatumReader<>(User.class);
                User payload = reader.read(null, decoder);
                user = payload;
                for (BenchmarkProducer producer: producers) {
                    User newUser = User.newBuilder(payload).build();
                    ((PravegaBenchmarkProducer)producer).setPayload(newUser);
                }
            } else if (serializationFormat == SerializationFormat.Json) {
                ObjectMapper objectMapper = new ObjectMapper();
                JSONUser payload = null;
                for (BenchmarkProducer producer : producers) {
                    payload = objectMapper.readValue(new File(producerWorkAssignment.payloadFile), JSONUser.class);
                    ((PravegaBenchmarkProducer) producer).setPayload(payload);
                }
                user = payload;
            } else if (serializationFormat == SerializationFormat.Protobuf) {
                Protobuf.PBUser.Builder builder = Protobuf.PBUser.newBuilder();
                try (FileReader fileReader = new FileReader(producerWorkAssignment.payloadFile)) {
                    JsonFormat.parser().merge(fileReader, builder);
                }
                for (BenchmarkProducer producer : producers) {
                    Protobuf.PBUser payload = Protobuf.PBUser.newBuilder(builder.build()).build();
                    ((PravegaBenchmarkProducer) producer).setPayload(payload);
                }
                user = builder.build();
            } else {
                throw new UnsupportedSerializationFormatException(serializationFormat);
            }

            if (this.timestampIncludedInEvent) {
                user.setEventTimestamp(System.currentTimeMillis());
            }
            ByteBuffer serialized = serializer.serialize(user);
            this.payloadSize = serialized.limit();
            log.info("Using payload file={} payloadSize={} bytes, message size: {}", producerWorkAssignment.payloadFile, payloadSize, producerWorkAssignment.messageSize);
            if (payloadSize != producerWorkAssignment.messageSize) {
                log.error("Payload {} is serialized into {} bytes, message size is {}", producerWorkAssignment.payloadFile, payloadSize, producerWorkAssignment.messageSize);
                throw new InvalidPayloadSerializedSizeException(serializationFormat, producerWorkAssignment.payloadFile, payloadSize, producerWorkAssignment.messageSize);
            }

            Lists.partition(producers, producersPerProcessor).stream()
                    .map(producersPerThread -> producersPerThread.stream()
                            .collect(Collectors.toMap(Function.identity(), assignKeyDistributor)))
                    .forEach(producersWithKeyDistributor -> submitProducersToExecutor(producersWithKeyDistributor, payloadSize));
        } else {
            final PayloadReader payloadReader = new FilePayloadReader(producerWorkAssignment.messageSize);
            byte[] payload = payloadReader.load(producerWorkAssignment.payloadFile);
            ByteBuffer serialized = serializer.serialize(payload);
            this.payloadSize = serialized.limit();

            Lists.partition(producers, producersPerProcessor).stream()
                    .map(producersPerThread -> producersPerThread.stream()
                            .collect(Collectors.toMap(Function.identity(), assignKeyDistributor)))
                    .forEach(producersWithKeyDistributor -> submitProducersToExecutorWithBytePayload(producersWithKeyDistributor,
                            payload));
        }

        if (payloadSize != producerWorkAssignment.messageSize) {
            log.warn("Payload {} is serialized into {} bytes, message size is {}", producerWorkAssignment.payloadFile, payloadSize, producerWorkAssignment.messageSize);
        }

    }

    @Override
    public void probeProducers() {
        if (need2ProbeProducers) {
            producers.forEach(
                    producer -> producer.sendAsync(Optional.of("key"), new byte[10]).thenRun(totalMessagesSent::increment));
        }
    }

    private void submitProducersToExecutor(Map<BenchmarkProducer, KeyDistributor> producersWithKeyDistributor,
            long payloadSize) {
        executor.submit(() -> {
            log.info("submitProducersToExecutor: # producers={}", producersWithKeyDistributor.size());
            try {
                while (!testCompleted) {
                    producersWithKeyDistributor.forEach((producer, producersKeyDistributor) -> {
                        if (rateLimiterEnabled) {
                            rateLimiter.acquire();
                        }
                        final long sendTime = System.nanoTime();
                        producer.sendAsync(Optional.ofNullable(producersKeyDistributor.next()))
                                .thenRun(() -> {
                            messagesSent.increment();
                            totalMessagesSent.increment();
                            messagesSentCounter.inc();

                            bytesSent.add(payloadSize);
                            bytesSentCounter.add(payloadSize);

                            long latencyMicros = Longs.constrainToRange(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sendTime), 0, publishLatencyMax);
                            publishLatencyRecorder.recordValue(latencyMicros);
                            cumulativePublishLatencyRecorder.recordValue(latencyMicros);
                            publishLatencyStats.registerSuccessfulEvent(latencyMicros, TimeUnit.MICROSECONDS);
                        }).exceptionally(ex -> {
                            log.warn("Write error on message", ex);
                            return null;
                        });
                    });
                }
            } catch (Throwable t) {
                log.error("Got error", t);
            }
        });
    }

    private void submitProducersToExecutorWithBytePayload(Map<BenchmarkProducer, KeyDistributor> producersWithKeyDistributor,
                                           byte[] payloadData) {
        executor.submit(() -> {
            log.info("submitProducersToExecutor: # producers={}", producersWithKeyDistributor.size());
            try {
                while (!testCompleted) {
                    producersWithKeyDistributor.forEach((producer, producersKeyDistributor) -> {
                        if (rateLimiterEnabled) {
                            rateLimiter.acquire();
                        }
                        final long sendTime = System.nanoTime();
                        producer.sendAsync(Optional.ofNullable(producersKeyDistributor.next()), payloadData)
                                .thenRun(() -> {
                                    messagesSent.increment();
                                    totalMessagesSent.increment();
                                    messagesSentCounter.inc();
                                    bytesSent.add(payloadData.length);
                                    bytesSentCounter.add(payloadData.length);

                                    long latencyMicros = Longs.constrainToRange(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sendTime), 0, publishLatencyMax);
                                    publishLatencyRecorder.recordValue(latencyMicros);
                                    cumulativePublishLatencyRecorder.recordValue(latencyMicros);
                                    publishLatencyStats.registerSuccessfulEvent(latencyMicros, TimeUnit.MICROSECONDS);
                                }).exceptionally(ex -> {
                            log.warn("Write error on message", ex);
                            return null;
                        });
                    });
                }
            } catch (Throwable t) {
                log.error("Got error", t);
            }
        });
    }

    @Override
    public void adjustPublishRate(double publishRate) {
        if(publishRate < 1.0) {
            rateLimiter.setRate(1.0);
            return;
        }
        rateLimiter.setRate(publishRate);
    }

    @Override
    public PeriodStats getPeriodStats() {
        PeriodStats stats = new PeriodStats();

        stats.messagesSent = messagesSent.sumThenReset();
        stats.bytesSent = bytesSent.sumThenReset();

        stats.messagesReceived = messagesReceived.sumThenReset();
        stats.bytesReceived = bytesReceived.sumThenReset();

        stats.totalMessagesSent = totalMessagesSent.sum();
        stats.totalMessagesReceived = totalMessagesReceived.sum();

        stats.publishLatency = publishLatencyRecorder.getIntervalHistogram();
        stats.endToEndLatency = endToEndLatencyRecorder.getIntervalHistogram();
        return stats;
    }

    @Override
    public CumulativeLatencies getCumulativeLatencies() {
        CumulativeLatencies latencies = new CumulativeLatencies();
        latencies.publishLatency = cumulativePublishLatencyRecorder.getIntervalHistogram();
        latencies.endToEndLatency = endToEndCumulativeLatencyRecorder.getIntervalHistogram();
        return latencies;
    }

    @Override
    public CountersStats getCountersStats() throws IOException {
        CountersStats stats = new CountersStats();
        stats.messagesSent = totalMessagesSent.sum();
        stats.messagesReceived = totalMessagesReceived.sum();
        return stats;
    }

    private void messageSizeReceived(int dataLength, long publishTimestamp) {
        messagesReceived.increment();
        totalMessagesReceived.increment();
        messagesReceivedCounter.inc();
        bytesReceived.add(dataLength);
        bytesReceivedCounter.add(dataLength);

        long now = System.currentTimeMillis();
        long endToEndLatencyMicros = Longs.constrainToRange(TimeUnit.MILLISECONDS.toMicros(now - publishTimestamp), 0, endToEndLatencyMax);
        if (endToEndLatencyMicros > 0) {
            endToEndCumulativeLatencyRecorder.recordValue(endToEndLatencyMicros);
            endToEndLatencyRecorder.recordValue(endToEndLatencyMicros);
            endToEndLatencyStats.registerSuccessfulEvent(endToEndLatencyMicros, TimeUnit.MICROSECONDS);
        }

        while (consumersArePaused) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // TODO get payload byte[]
    @Override
    public void messageReceived(byte[] data, long publishTimestamp) {
        this.messageSizeReceived(data.length, publishTimestamp);
    }

    @Override
    public void eventReceived(long publishTimestamp) {
        this.messageSizeReceived(payloadSize, publishTimestamp);
    }

    @Override
    public void pauseConsumers() throws IOException {
        consumersArePaused = true;
        log.info("Pausing consumers");
    }

    @Override
    public void resumeConsumers() throws IOException {
        consumersArePaused = false;
        log.info("Resuming consumers");
    }

    @Override
    public void resetStats() throws IOException {
        publishLatencyRecorder.reset();
        cumulativePublishLatencyRecorder.reset();
        endToEndLatencyRecorder.reset();
        endToEndCumulativeLatencyRecorder.reset();
    }

    @Override
    public void stopAll() throws IOException {
        testCompleted = true;
        consumersArePaused = false;

        publishLatencyRecorder.reset();
        cumulativePublishLatencyRecorder.reset();
        endToEndLatencyRecorder.reset();
        endToEndCumulativeLatencyRecorder.reset();

        messagesSent.reset();
        bytesSent.reset();
        messagesReceived.reset();
        bytesReceived.reset();
        totalMessagesSent.reset();
        totalMessagesReceived.reset();

        try {
            // TODO: Must wait for all tasks in executor to complete. For now, sleep a while.
            Thread.sleep(5000);

            producers.parallelStream().forEach((BenchmarkProducer benchmarkProducer) -> {
                try {
                    benchmarkProducer.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            producers.clear();

            consumers.parallelStream().forEach((BenchmarkConsumer benchmarkConsumer) -> {
                try {
                    benchmarkConsumer.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            consumers.clear();

            if (benchmarkDriver != null) {
                benchmarkDriver.close();
                benchmarkDriver = null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }

    private static final Logger log = LoggerFactory.getLogger(LocalWorker.class);
}
