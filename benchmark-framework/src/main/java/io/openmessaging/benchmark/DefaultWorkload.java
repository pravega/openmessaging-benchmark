package io.openmessaging.benchmark;

import io.openmessaging.benchmark.utils.payload.FilePayloadReader;
import io.openmessaging.benchmark.utils.payload.PayloadReader;

public class DefaultWorkload extends Workload {

    private PayloadReader payloadReader = null;

    public String payloadFile;

    public byte[] getPayload() {
        if (payloadReader == null) {
            payloadReader = new FilePayloadReader(messageSize);
        }
        return payloadReader.load(payloadFile);
    }
}
