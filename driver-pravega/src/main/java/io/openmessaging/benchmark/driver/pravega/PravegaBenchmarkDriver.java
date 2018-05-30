package io.openmessaging.benchmark.driver.pravega;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.stats.StatsLogger;

public class PravegaBenchmarkDriver implements BenchmarkDriver {
    private ClientConfig config;
    private StreamManager manager;

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        config = ClientConfig.builder().build();
        manager = StreamManager.create(config);
    }

    @Override
    public String getTopicNamePrefix() {
        return "pravega-benchmark";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
         manager.createStream("benchmark", topic,
                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(partitions)).build());
         return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        return null;
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
