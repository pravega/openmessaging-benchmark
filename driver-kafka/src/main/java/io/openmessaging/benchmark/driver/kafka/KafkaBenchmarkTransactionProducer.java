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
package io.openmessaging.benchmark.driver.kafka;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.openmessaging.benchmark.driver.BenchmarkProducer;

public class KafkaBenchmarkTransactionProducer implements BenchmarkProducer {

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;
    private int counter;
    private int eventsPerTransaction;
    private boolean isClosed;

    public KafkaBenchmarkTransactionProducer(KafkaProducer<String, byte[]> producer, String topic,
            int eventsPerTransaction) {
        this.producer = producer;
        this.topic = topic;
        this.eventsPerTransaction = eventsPerTransaction;
        this.producer.initTransactions();
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        synchronized (this) {
            try {
                if (isClosed) {
                    future.complete(null);
                    return future;
                }

                if (counter == 0) {
                    producer.beginTransaction();
                }

                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key.orElse(null), payload);

                if (++counter >= eventsPerTransaction) {
                    producer.commitTransaction();
                    counter = 0;
                }
            } catch (Exception e) {
                producer.abortTransaction();
                future.completeExceptionally(e);
                return future;
            }
        }
        future.complete(null);
        return future;
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            try {
                producer.abortTransaction();
                isClosed = true;
            } catch (Exception e) {
                // perhaps there is no transaction started, do nothing;
            }
        }
        producer.close();
    }

}
