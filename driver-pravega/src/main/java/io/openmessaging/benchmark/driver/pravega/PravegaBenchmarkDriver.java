package io.openmessaging.benchmark.driver.pravega;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.pravega.client.ClientConfig;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.stats.StatsLogger;

public class PravegaBenchmarkDriver implements BenchmarkDriver {
    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        ClientConfig config = ClientConfig.builder().build();
    }

    @Override
    public String getTopicNamePrefix() {
        return null;
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        return null;
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
