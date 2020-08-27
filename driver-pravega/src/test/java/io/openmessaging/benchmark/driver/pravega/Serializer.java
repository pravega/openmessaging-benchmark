package io.openmessaging.benchmark.driver.pravega;

import java.nio.ByteBuffer;

public interface Serializer {
    byte[] serialize(Object value);
    Object deserialize(Object value);
}
