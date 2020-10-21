package io.openmessaging.benchmark.driver.pravega;

import io.openmessaging.benchmark.driver.pravega.schema.generated.protobuf.Protobuf;
import io.openmessaging.benchmark.driver.pravega.schema.json.JSONAddressEntry;
import io.openmessaging.benchmark.driver.pravega.schema.json.JSONKeyValue;
import io.openmessaging.benchmark.driver.pravega.schema.json.JSONUser;
import io.pravega.client.stream.Serializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static io.openmessaging.benchmark.driver.pravega.RandomStringUtils.generateRandomString2;

@Slf4j
public class ProtobufUtils {


    public static final ProtobufUtils INSTANCE = new ProtobufUtils();

    public Pair<Protobuf.PBUser, Integer> generateUser(Serializer<Protobuf.PBUser> serializer, int valueSize, int values, int valueSize2, int values2) {
        return innerGenerateUser(serializer, valueSize, values, valueSize2, values2, 0, 0);
    }

    public Pair<Protobuf.PBUser, Integer> innerGenerateUser(Serializer<Protobuf.PBUser> serializer, int valueSize, int values, int valueSize2, int values2, int valueSize3, int values3) {
        List<Protobuf.PBKeyValue> keyValues = new LinkedList<>();

        for (int i=0; i < values; i++) {
            String name = generateRandomString2(valueSize);
            String value = generateRandomString2(valueSize);
            Protobuf.PBKeyValue keyValue = Protobuf.PBKeyValue.newBuilder().setName(name).setValue(value).build();
            keyValues.add(keyValue);
        }
        List<Protobuf.PBKeyValue> keyValues2 = new LinkedList<>();
        for (int i=0; i < values2; i++) {
            String name = generateRandomString2(valueSize2);
            String value = generateRandomString2(valueSize2);
            Protobuf.PBKeyValue keyValue2 = Protobuf.PBKeyValue.newBuilder().setName(name).setValue(value).build();
            keyValues2.add(keyValue2);
        }
        List<Protobuf.PBKeyValue> keyValues3 = new LinkedList<>();
        for (int i=0; i < values3; i++) {
            String name = generateRandomString2(valueSize3);
            String value = generateRandomString2(valueSize3);
            Protobuf.PBKeyValue keyValue3 = Protobuf.PBKeyValue.newBuilder().setName(name).setValue(value).build();
            keyValues3.add(keyValue3);
        }

        Protobuf.PBAddressEntry addressEntry = Protobuf.PBAddressEntry.newBuilder()
                .setCity("Perth")
                .setStreetAddress("Avenue ")
                .setPostalCode(40191).build();
        // Basic user is serialized to 100b
        Protobuf.PBUser.Builder builder = Protobuf.PBUser.newBuilder()
                .setUserId("1603186293711")
                .setBiography("Australian science fiction writer...")
                .setEventTimestampInner(System.currentTimeMillis())
                .setName("Greg Igan")
                .setAge(59)
                .addAddress(addressEntry);

        if (!keyValues.isEmpty()) {
            builder = builder.addAllKeyValues(keyValues);
        }
        if (!keyValues2.isEmpty()) {
            builder = builder.addAllKeyValues2(keyValues2);
        }
        if (!keyValues3.isEmpty()) {
            builder = builder.addAllKeyValues3(keyValues3);
        }
        Protobuf.PBUser user = builder.build();

        ByteBuffer byteBuffer = serializer.serialize(user);
        int payloadSize = byteBuffer.limit();
        log.info("Serializer:{}", serializer);
        log.info("valueSize:{} values:{}, valueSize2:{} values2:{}", valueSize, values, valueSize2, values2);
        log.info("ByteBuffer capacity {}", byteBuffer.capacity());
        log.info("ByteBuffer limit {}", byteBuffer.limit());
        return Pair.of(user, payloadSize);
    }
}
