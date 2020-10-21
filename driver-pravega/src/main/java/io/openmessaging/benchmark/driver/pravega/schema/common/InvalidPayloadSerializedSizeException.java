package io.openmessaging.benchmark.driver.pravega.schema.common;

import io.pravega.schemaregistry.contract.data.SerializationFormat;

public class InvalidPayloadSerializedSizeException extends IllegalArgumentException {
    public InvalidPayloadSerializedSizeException(SerializationFormat format, String payloadFile, int payloadSize, int messageSize) {
        super(String.format("Payload file %s is serialized with serialization format: %s into byte array of %d, message size specified is %d", payloadFile, format.getFullTypeName(), payloadSize, messageSize));
    }
}
