package io.openmessaging.benchmark.driver.pravega;

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.pravega.client.stream.EventStreamWriter;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class PravegaBenchmarkProducer implements BenchmarkProducer {

    private final EventStreamWriter producer;

    public PravegaBenchmarkProducer(EventStreamWriter pravegaProducer) {
        this.producer = pravegaProducer;
    }
    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        return producer.writeEvent(key.get(), payload);
    }

    @Override
    public void close() throws Exception {
        producer.close();

    }
}
