package io.openmessaging.benchmark.driver.pravega;

import io.openmessaging.benchmark.driver.pravega.schema.json.JSONAddressEntry;
import io.openmessaging.benchmark.driver.pravega.schema.json.JSONKeyValue;
import io.openmessaging.benchmark.driver.pravega.schema.json.JSONUser;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static io.openmessaging.benchmark.driver.pravega.RandomStringUtils.generateRandomString2;
import static io.openmessaging.benchmark.driver.pravega.RandomStringUtils.generateRandomString3;

@Slf4j
public class JSONUtils {


    public static final JSONUtils INSTANCE = new JSONUtils();

    public Pair<JSONUser, Integer> generateUser(Serializer<JSONUser> serializer, int valueSize, int values, int valueSize2, int values2) {
        return innerGenerateUser(serializer, valueSize, values, valueSize2, values2, 0, 0);
    }

    public Pair<JSONUser, Integer> innerGenerateUser(Serializer<JSONUser> serializer, int valueSize, int values, int valueSize2, int values2, int valueSize3, int values3) {
        List<JSONKeyValue> keyValues = new LinkedList<>();

        for (int i=0; i < values; i++) {
            String name = generateRandomString3(valueSize);
            String value = generateRandomString3(valueSize);
            JSONKeyValue keyValue = JSONKeyValue.builder().name(name).value(value).build();
            keyValues.add(keyValue);
        }
        List<JSONKeyValue> keyValues2 = new LinkedList<>();
        for (int i=0; i < values2; i++) {
            String name = generateRandomString3(valueSize2);
            String value = generateRandomString3(valueSize2);
            JSONKeyValue keyValue2 = JSONKeyValue.builder().name(name).value(value).build();
            keyValues2.add(keyValue2);
        }
        List<JSONKeyValue> keyValues3 = new LinkedList<>();
        for (int i=0; i < values3; i++) {
            String name = generateRandomString3(valueSize3);
            String value = generateRandomString3(valueSize3);
            JSONKeyValue keyValue3 = JSONKeyValue.builder().name(name).value(value).build();
            keyValues3.add(keyValue3);
        }
        // Basic user is serialized to 100b
        JSONUser.JSONUserBuilder builder = JSONUser.builder()
                .userId("1")
                .biography(null)
                .eventTimestamp(System.currentTimeMillis())
                .name("G.E")
                .keyValues(null)
                .keyValues2(null)
                .keyValues3(null)
                .address(Collections.singletonList(JSONAddressEntry.builder()
                        .postalCode(1).build()));

        if (!keyValues.isEmpty()) {
            builder = builder.keyValues(keyValues);
        }
        if (!keyValues2.isEmpty()) {
            builder = builder.keyValues2(keyValues2);
        }
        if (!keyValues3.isEmpty()) {
            builder = builder.keyValues3(keyValues3);
        }
        JSONUser user = builder.build();

        ByteBuffer byteBuffer = serializer.serialize(user);
        int payloadSize = byteBuffer.limit();
        log.info("Serializer:{}", serializer);
        log.info("valueSize:{} values:{}, valueSize2:{} values2:{}", valueSize, values, valueSize2, values2);
        log.info("ByteBuffer capacity {}", byteBuffer.capacity());
        log.info("ByteBuffer limit {}", byteBuffer.limit());
        return Pair.of(user, payloadSize);
    }
}
