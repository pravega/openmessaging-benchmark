package io.openmessaging.benchmark.driver.pravega;

import com.google.common.math.IntMath;
import io.openmessaging.benchmark.driver.pravega.schema.generated.avro.*;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.apache.commons.text.RandomStringGenerator;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.RoundingMode;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.openmessaging.benchmark.driver.pravega.RandomStringUtils.generateRandomString2;
import static java.nio.file.Files.readAllBytes;

@Slf4j
public class AvroUtils {

    private final static String resourcesPath = "src/test/resources/schema-registry/avro";

    private final Schema schema = AvroSchema.of(User.class).getSchema();

    public static final AvroUtils INSTANCE = new AvroUtils();

    private ByteBuffer timestampAndPayload;

    private final  User user = User.newBuilder()
            .setUserId("Iu6AxdBYGR4A0wspR9BYHA")
            .setBiography("Greg Egan was born 20 August 1961.")
            .setEventTimestamp(System.currentTimeMillis())
            .setName("Greg Egan")
            .setKeyValues(null)
            .setKeyValues2(null)
            .setKeyValues3(null)
            .setAddress(AddressEntry.newBuilder().setCity("Perth")
                    .setPostalCode(5018)
                    .setStreetAddress("4/19 Gardner Road").build()).build();

    public Pair<User, Integer> generateUser(Serializer serializer, int valueSize, int values, int valueSize2, int values2) throws NoSuchAlgorithmException {
        return innerGenerateUser(serializer, valueSize, values, valueSize2, values2, 0, 0);
    }

    public Pair<User, Integer> innerGenerateUser(Serializer serializer, int valueSize, int values, int valueSize2, int values2, int valueSize3, int values3) throws NoSuchAlgorithmException {
        List<KeyValue> keyValues = new LinkedList<>();

        for (int i=0; i < values; i++) {
            String name = generateRandomString2(valueSize);
            String value = generateRandomString2(valueSize);
            KeyValue keyValue = KeyValue.newBuilder().setName(name).setValue(value).build();
            keyValues.add(keyValue);
        }
        List<KeyValue2> keyValues2 = new LinkedList<>();
        for (int i=0; i < values2; i++) {
            String name = generateRandomString2(valueSize2);
            String value = generateRandomString2(valueSize2);
            KeyValue2 keyValue2 = KeyValue2.newBuilder().setName(name).setValue(value).build();
            keyValues2.add(keyValue2);
        }
        List<KeyValue3> keyValues3 = new LinkedList<>();
        for (int i=0; i < values3; i++) {
            String name = generateRandomString2(valueSize3);
            String value = generateRandomString2(valueSize3);
            KeyValue3 keyValue2 = KeyValue3.newBuilder().setName(name).setValue(value).build();
            keyValues3.add(keyValue2);
        }
        // Basic user is serialized to 100b
        User user = User.newBuilder()
                .setUserId("Iu6AxdBYGR4")
                .setBiography("Greg Egan was born 20 August 1961.")
                .setEventTimestamp(System.currentTimeMillis())
                .setName("Greg Egan")
                .setKeyValues(keyValues)
                .setKeyValues2(keyValues2)
                .setKeyValues3(keyValues3)
                .setAddress(AddressEntry.newBuilder().setCity("Perth")
                        .setPostalCode(5018)
                        .setStreetAddress("19 Gardner Road").build()).build();

        ByteBuffer byteBuffer = serializer.serialize(user);
        int payloadSize = byteBuffer.limit();
        log.info("Serializer:{}", serializer);
        log.info("valueSize:{} values:{}, valueSize2:{} values2:{}", valueSize, values, valueSize2, values2);
        log.info("ByteBuffer capacity {}", byteBuffer.capacity());
        log.info("ByteBuffer limit {}", byteBuffer.limit());
        return Pair.of(user, payloadSize);
    }

    public void generateJSON(int messageSize, Serializer serializer) throws NoSuchAlgorithmException, IOException, URISyntaxException {
        // {key:69b value:69b} keyValue string = +500 serialized bytes
        int inputBytes = 100;
        int serialized = 500;

        int values = IntMath.divide(messageSize, serialized, RoundingMode.FLOOR);
        int left = messageSize - values * serialized;
        log.info("size {} values:{} left: {}", values * serialized, values, left);
        generateUser(messageSize, serializer, inputBytes, values, 0, 0, 0, 0);
    }


    public Pair<User, Integer> generateUser(int messageSize, Serializer serializer, int valueSize, int values, int valueSize2, int values2, int valueSize3, int values3) throws IOException, NoSuchAlgorithmException {
        String payloadFile = resourcesPath + String.format("/payload/generated/user-payload-%d.json", messageSize);
        Pair<User, Integer> pair = innerGenerateUser(serializer, valueSize, values, valueSize2, values2, valueSize3, values3);
        int payloadSize = pair.getRight();
        User user = pair.getLeft();
        final FileOutputStream bout = new FileOutputStream(payloadFile);
        Encoder encoder = EncoderFactory.get().jsonEncoder(schema, bout, true);
        DatumWriter<User> writer = new SpecificDatumWriter<>(User.class);
        writer.write(user, encoder);
        encoder.flush();
        bout.close();
        String payloadJSON = FileUtils.readFileToString(new File(payloadFile), "UTF-8");
        JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, payloadJSON);
        SpecificDatumReader<User> reader = new SpecificDatumReader<>(User.class);
        User userDeserialized = reader.read(user, decoder);
        Assert.assertNotNull(userDeserialized);
        log.info("{}", userDeserialized);

        return Pair.of(user, payloadSize);
    }

    public int serializeAvro(String payloadFile, Schema schema, Serializer<User> serializer, Serializer<User> deserializer, int serializeTimes) throws IOException {
        User user = read(payloadFile, schema);
        user.setEventTimestamp(System.currentTimeMillis());
        List<Pair<Double, Double>> serialize = new LinkedList<>();
        int payloadSize = 0;
        for (int i = 0; i < serializeTimes; i++) {
            long before = System.nanoTime();
            ByteBuffer serialized = serializer.serialize(user);
            double serializeMillis = (System.nanoTime() - before) / (double) TimeUnit.MILLISECONDS.toNanos(1);
            payloadSize = serialized.limit();
            //log.info("Serialized:\n{}", new String(serialized.array()));
            before = System.nanoTime();
            User deserializedUser = deserializer.deserialize(serialized);
            long deserializeNanos = System.nanoTime() - before;
            double deserializeMillis = deserializeNanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
            //log.info("Deserialized user: {}", deserializedUser);
            log.info("Payload {} bytes, serialize: {} ms, deserialize: {} ms", payloadSize, serializeMillis, deserializeMillis);
            //log.info("Deserialized user: {}", deserializedUser);
            serialize.add(Pair.of(serializeMillis, deserializeMillis));
        }
        return payloadSize;
    }

    public void serializeAvroGeneric(int serializeTimes, User user) {
        log.info("Test SpecificDatumWriter/SpecificDatumReader with generated User class");
        DatumWriter<User> writer = new SpecificDatumWriter<>(User.class);
        DatumReader<User> reader = new SpecificDatumReader<>(User.class);
        int payloadSize;
        User user2;
        for (int i = 0; i < serializeTimes; i++) {
            // serialize
            long before = System.nanoTime();
            byte[] serialized = toBytesGeneric(user, writer);
            long serializeNanos = System.nanoTime() - before;
            double serializeMillis = serializeNanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
            payloadSize = serialized.length;
            // deserialize
            before = System.nanoTime();
            user2 = fromBytesGeneric(serialized, reader, user);
            long deserializeNanos = System.nanoTime() - before;
            double deserializeMillis = deserializeNanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
            log.info("Payload {} bytes, serialize: {}ms, deserialize: {}ms", payloadSize, serializeMillis, deserializeMillis);
        }
    }

    public void serializeByte(String payloadFile, int serializeTimes) throws IOException {
        ByteBufferSerializer serializer = new ByteBufferSerializer();
        byte[] payloadData = readAllBytes(new File(payloadFile).toPath());
        List<Pair<Double, Double>> serialize = new LinkedList<>();
        int payloadSize;
        for (int i = 0; i < serializeTimes; i++) {
            ByteBuffer payloadBuffer = includeTimestampInPayload(payloadData);
            payloadSize = payloadBuffer.array().length;
            long before = System.nanoTime();
            ByteBuffer serialized = serializer.serialize(payloadBuffer);
            long serializeNanos = System.nanoTime() - before;
            double serializeMillis = serializeNanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
            before = System.nanoTime();
            ByteBuffer deserialized = serializer.deserialize(serialized);
            long deserializeNanos = System.nanoTime() - before;
            double deserializeMillis = deserializeNanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
            log.info("Payload {} bytes, serialize: {}ms, deserialize: {}ms", payloadSize, serializeMillis, deserializeMillis);
            serialize.add(Pair.of(serializeMillis, deserializeMillis));
        }
    }

    public ByteBuffer includeTimestampInPayload(byte[] payload) {
        if (timestampAndPayload == null || timestampAndPayload.limit() != Long.BYTES + payload.length) {
            timestampAndPayload = ByteBuffer.allocate(Long.BYTES + payload.length);
        } else {
            timestampAndPayload.position(0);
        }
        timestampAndPayload.putLong(System.currentTimeMillis()).put(payload).flip();
        return timestampAndPayload;
    }

    public User read(String payloadJSONFile, Schema schema) throws IOException {
        String payloadJSON = FileUtils.readFileToString(new File(payloadJSONFile), "UTF-8");
        JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, payloadJSON);
        SpecificDatumReader<User> reader = new SpecificDatumReader<>(User.class);
        return reader.read(user, decoder);
    }

    public static <V> byte[] toBytesGeneric(final V v, DatumWriter<V> writer) {
        final ByteArrayOutputStream bout = new ByteArrayOutputStream();
        final BinaryEncoder binEncoder = EncoderFactory.get().binaryEncoder(bout, null);
        try {
            writer.write(v, binEncoder);
            binEncoder.flush();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return bout.toByteArray();
    }

    public static <V> V fromBytesGeneric(final byte[] serialized, final DatumReader<V> reader, final V oldV) {
        final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(serialized, null);
        try {
            return reader.read(oldV, decoder);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }


}
