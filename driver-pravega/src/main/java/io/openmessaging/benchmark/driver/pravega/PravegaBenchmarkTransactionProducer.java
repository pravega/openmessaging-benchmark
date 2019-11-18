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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.concurrent.atomic.AtomicInteger;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.UUID;

public class PravegaBenchmarkTransactionProducer implements BenchmarkProducer {
    private static final Logger log = LoggerFactory.getLogger(PravegaBenchmarkProducer.class);

    private TransactionalEventStreamWriter<ByteBuffer> transactionWriter;

    private final boolean includeTimestampInEvent;

    // If null, a transaction has not been started.
    @GuardedBy("this")
    private Transaction<ByteBuffer> transaction;
    private final int eventsPerTransaction;
    private int eventCount = 0;

    public PravegaBenchmarkTransactionProducer(String streamName, EventStreamClientFactory clientFactory,
            boolean includeTimestampInEvent, boolean enableConnectionPooling, int eventsPerTransaction) {
        log.info("PravegaBenchmarkProducer: BEGIN: streamName={}", streamName);

        final String writerId = UUID.randomUUID().toString();
        transactionWriter = clientFactory.createTransactionalEventWriter(writerId, streamName,
                new ByteBufferSerializer(),
                EventWriterConfig.builder().enableConnectionPooling(enableConnectionPooling).build());
        this.eventsPerTransaction = eventsPerTransaction;
        this.includeTimestampInEvent = includeTimestampInEvent;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        ByteBuffer payloadToWrite;
        if (includeTimestampInEvent) {
            // We must create a new buffer for the combined event timestamp and payload.
            // This requires copying the entire payload.
            long eventTimestamp = System.currentTimeMillis();
            payloadToWrite = ByteBuffer.allocate(Long.BYTES + payload.length);
            payloadToWrite.putLong(eventTimestamp);
            payloadToWrite.put(payload);
            payloadToWrite.flip();
        } else {
            payloadToWrite = ByteBuffer.wrap(payload);
        }
        try {
            synchronized (this) {
                if (transaction == null) {
                    transaction = transactionWriter.beginTxn();
                }
                if (key.isPresent()) {
                    transaction.writeEvent(key.get(), payloadToWrite);
                } else {
                    transaction.writeEvent(payloadToWrite);
                }
                if (++eventCount >= eventsPerTransaction) {
                    eventCount = 0;
                    transaction.commit();
                    transaction = null;
                }
            }
        } catch (TxnFailedException e) {
            throw new RuntimeException("Transaction Write data failed ", e);
        }
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        return future;
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (transaction != null) {
                transaction.abort();
                transaction = null;
            }
        }
        transactionWriter.close();
    }
}
