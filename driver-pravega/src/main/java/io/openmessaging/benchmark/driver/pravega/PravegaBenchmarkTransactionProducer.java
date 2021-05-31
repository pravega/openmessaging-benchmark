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

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.*;
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
    private ByteBuffer timestampAndPayload;
    // -- Additional measurements
    private long noneToOpenStartEpoch;
    private long noneToOpenEndEpoch;

    private ExecutorService executorService;

    /**
     * TODO: We must create thread pool with N tasks, N = committing transactions.
     * Thread pool must check statuses of them in non-blocking fashion.
     * We might have a transaction queue. We'd go through the queue and check the status, remove from the queue COMMITTED transactions afterwards.
     */
    public static class PollingJob implements Callable<Void> {
        private Transaction<ByteBuffer> transaction;
        private final long noneToOpenStartEpoch;
        private final long noneToOpenEndEpoch;
        private final long commitFinishedEpoch;
        private final long commitProcessStartEpoch;

        public PollingJob(final long noneToOpenStartEpoch, final long noneToOpenEndEpoch, final long commitProcessStartEpoch, final long commitFinishedEpoch, Transaction transaction) {
            this.noneToOpenStartEpoch = noneToOpenStartEpoch;
            this.noneToOpenEndEpoch = noneToOpenEndEpoch;
            this.commitProcessStartEpoch = commitProcessStartEpoch;
            this.commitFinishedEpoch = commitFinishedEpoch;
            this.transaction = transaction;
        }

        @Override
        // TODO: Scheduled feature? If not committed - return
        public Void call() throws Exception {

            final long committedFinishedEpoch = getTimeStatusReached(this.transaction, Transaction.Status.COMMITTED);
            // NONE <-> OPEN
            final long durationNoneToOpenMs = (this.noneToOpenEndEpoch - this.noneToOpenStartEpoch) / (long) 1000000;
            // OPEN <-> COMMITTING (including writes)
            final long durationOpenToCommittingMs = (this.commitFinishedEpoch - this.noneToOpenEndEpoch) / (long) 1000000;
            // COMMITTING <-> COMMITTED
            final long durationCommittingToCommittedMs = (committedFinishedEpoch - this.commitFinishedEpoch) / (long) 1000000;
            // COMMIT SPECIFIC
            final long commitProcessOnly = (commitFinishedEpoch - commitProcessStartEpoch) / (long) 1000000;
            // WRITES ONLY
            final long writeOnlyDurationMs = (commitProcessStartEpoch - this.noneToOpenEndEpoch) / (long) 1000000;
            PravegaBenchmarkTransactionProducer.log.debug("Transaction---" + transaction.getTxnId() + "---OPEN---" + durationNoneToOpenMs +
                    "---COMMITTING---" + durationOpenToCommittingMs + "---COMMITTED---" +
                    durationCommittingToCommittedMs + "---EPOCH---" + System.currentTimeMillis() +
                    "---COMMITONLY---" + commitProcessOnly + "---WRITEONLY---" + writeOnlyDurationMs);
            return null;
        }

        /**
         * Ensures @param transaction reached given @param status
         * @return system time when given status had been reached.
         */
        private long getTimeStatusReached(Transaction transaction, Transaction.Status status) {
            while(transaction.checkStatus() != status) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return 0;
                }
            }
            return System.nanoTime();
        }
    }

    public PravegaBenchmarkTransactionProducer(String streamName, EventStreamClientFactory clientFactory,
            boolean includeTimestampInEvent, boolean enableConnectionPooling, int eventsPerTransaction) {
        log.info("PravegaBenchmarkProducer: BEGIN: streamName={}", streamName);

        final String writerId = UUID.randomUUID().toString();
        this.executorService = Executors.newFixedThreadPool(15);
        transactionWriter = clientFactory.createTransactionalEventWriter(writerId, streamName,
                new ByteBufferSerializer(),
                EventWriterConfig.builder().enableConnectionPooling(enableConnectionPooling).build());
        this.eventsPerTransaction = eventsPerTransaction;
        this.includeTimestampInEvent = includeTimestampInEvent;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        try {
            if (transaction == null) {
                this.noneToOpenStartEpoch = System.nanoTime();
                transaction = transactionWriter.beginTxn();
                // beginTxn() is Synchronous => do not wait for status OPEN implicitly
                this.noneToOpenEndEpoch = System.nanoTime();
            }
            final boolean emptyTxnRequested = (eventsPerTransaction == 0);
            if (!emptyTxnRequested) {
                if (includeTimestampInEvent) {
                    if (timestampAndPayload == null || timestampAndPayload.limit() != Long.BYTES + payload.length) {
                        timestampAndPayload = ByteBuffer.allocate(Long.BYTES + payload.length);
                    } else {
                        timestampAndPayload.position(0);
                    }
                    timestampAndPayload.putLong(System.currentTimeMillis()).put(payload).flip();
                    writeEvent(key, timestampAndPayload);
                } else {
                    writeEvent(key, ByteBuffer.wrap(payload));
                }
            }

            if (++eventCount >= eventsPerTransaction) {
                eventCount = 0;
                final long commitProcessStartEpoch = System.nanoTime();
                transaction.commit();
                final long commitFinishedEpoch = System.nanoTime();
                // BegintTxn(), commit(), write()
                final long beginCommitDurMs = (this.noneToOpenEndEpoch - this.noneToOpenStartEpoch) / (long) 1000000;
                final long writeExclusiveDurMs = (commitProcessStartEpoch - this.noneToOpenEndEpoch) / (long) 1000000;
                final long commitExclusiveDurMs = (commitFinishedEpoch - commitProcessStartEpoch) / (long) 1000000;
                log.info("---BEGINTXN---" + beginCommitDurMs +
                        "---WRITE---" + writeExclusiveDurMs + "---COMMITT---" +
                        commitExclusiveDurMs + "---EPOCH---" + System.currentTimeMillis());
//                 this.executorService.submit(new PollingJob(this.noneToOpenStartEpoch, this.noneToOpenEndEpoch, commitProcessStartEpoch, commitFinishedEpoch, this.transaction));

                transaction = null;
            }
        } catch (TxnFailedException e) {
            throw new RuntimeException("Transaction Write data failed ", e);
        }
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        return future;
    }

    private void writeEvent(Optional<String> key, ByteBuffer payload) throws TxnFailedException {
        if (key.isPresent()) {
            transaction.writeEvent(key.get(), payload);
        } else {
            transaction.writeEvent(payload);
        }
    }

    @Override
    public void close() throws Exception {
        this.executorService.shutdown();
        executorService.awaitTermination(30, TimeUnit.MILLISECONDS);
        synchronized (this) {
            if (transaction != null) {
                transaction.abort();
                transaction = null;
            }
        }
        transactionWriter.close();
    }

}
