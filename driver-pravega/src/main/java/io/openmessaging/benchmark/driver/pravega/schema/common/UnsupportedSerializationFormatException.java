package io.openmessaging.benchmark.driver.pravega.schema.common;

import io.pravega.schemaregistry.contract.data.SerializationFormat;

public class UnsupportedSerializationFormatException extends UnsupportedOperationException {

    public UnsupportedSerializationFormatException(SerializationFormat format) {
        super(String.format("Serialization format: %s is not supported yet for Pravega driver", format.getFullTypeName()));
    }
}
