package io.openmessaging.benchmark.driver.pravega;

import io.openmessaging.benchmark.driver.pravega.config.PravegaConfig;
import io.openmessaging.benchmark.driver.pravega.config.SchemaRegistryConfig;
import io.openmessaging.benchmark.driver.pravega.testobj.generated.AddressEntry;
import io.openmessaging.benchmark.driver.pravega.testobj.generated.User;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.nio.file.Files.readAllBytes;


//TODO make unit test, not requiring standalone pravega running along with registry
@Slf4j
public class AvroSerializerTest {

    private ByteBuffer timestampAndPayload;

    private final User user = User.newBuilder()
            .setUserId("Iu6AxdBYGR4A0wspR9BYHA")
            .setBiography("Greg Egan was born 20 August 1961.")
            .setEventTimestamp(System.currentTimeMillis())
            .setName("Greg Egan")
            .setAddress(AddressEntry.newBuilder().setCity("Perth")
                    .setPostalCode(5018)
                    .setStreetAddress("4/19 Gardner Road").build()).build();

    @Ignore
    @Test
    public void testWithStandalonePravega() throws IOException, URISyntaxException {
        serializeAvro("src/test/resources/schema-registry/pravega-standalone.yaml", "src/test/resources/schema-registry/user-payload-100b.json", 10);
    }

    @Test
    public void testSerializeAvro() throws IOException, URISyntaxException {
        serializeAvro("src/test/resources/schema-registry/pravega.yaml", "src/test/resources/schema-registry/user-payload-100b.json", 100);
    }

    @Test
    public void testSerializeJSON() throws IOException {
        String payloadFile = "src/test/resources/schema-registry/user-payload-100b.json";
        String schemaFile = "src/test/resources/schema-registry/user.avsc";
        Schema schema = new Schema.Parser().parse(new File(schemaFile));
        final FileOutputStream bout = new FileOutputStream("src/test/resources/schema-registry/user-payload-100b.json");
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
    }

    @Test
    public void testDefaultAvroSerializer() {
        log.info("Test SpecificDatumWriter/SpecificDatumReader with generated User class");
        int serializeTimes = 100;
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

    @Test
    public void testByteSerializer() throws IOException {
        serializeByte("src/test/resources/schema-registry/payload-100b.data", 100);
    }

    private void serializeByte(String payloadFile, int serializeTimes) throws IOException {
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

    private ByteBuffer includeTimestampInPayload(byte[] payload) {
        if (timestampAndPayload == null || timestampAndPayload.limit() != Long.BYTES + payload.length) {
            timestampAndPayload = ByteBuffer.allocate(Long.BYTES + payload.length);
        } else {
            timestampAndPayload.position(0);
        }
        timestampAndPayload.putLong(System.currentTimeMillis()).put(payload).flip();
        return timestampAndPayload;
    }

    private void serializeAvro(String driverConfigFile, String payloadFile, int serializeTimes) throws IOException, URISyntaxException {
        File driverfile = new File(driverConfigFile);
        PravegaConfig driverConfig = PravegaBenchmarkDriver.readConfig(driverfile);
        SchemaRegistryConfig schemaRegistryConfig = driverConfig.schemaRegistry;
        log.info("schema file: {}", schemaRegistryConfig.schemaFile);
        SchemaRegistryClientConfig config;
        try {
            config = SchemaRegistryClientConfig.builder()
                    .schemaRegistryUri(new URI(schemaRegistryConfig.schemaRegistryURI)).build();
        } catch (URISyntaxException e) {
            log.error("schemaRegistryURI {} is invalid.", schemaRegistryConfig.schemaRegistryURI, e);
            throw e;
        }
        // Create serializer and deserializer
        SerializerConfig serializerConfig = SerializerConfig.builder()
                .groupId(schemaRegistryConfig.groupId).registryConfig(config)
                .createGroup(SerializationFormat.Avro).registerSchema(true)
                .build();
        AvroSchema<User> schema = AvroSchema.of(User.class);
        Serializer<User> serializer = SerializerFactory
                .avroSerializer(serializerConfig, schema);
        Serializer<User> deserializer = SerializerFactory.avroDeserializer(
                serializerConfig, schema);
        User user = read(payloadFile, schema.getSchema());
        user.setEventTimestamp(System.currentTimeMillis());
        List<Pair<Double, Double>> serialize = new LinkedList<>();
        int payloadSize;
        for (int i = 0; i < serializeTimes; i++) {
            long before = System.nanoTime();
            ByteBuffer serialized = serializer.serialize(user);
            double serializeMillis = (System.nanoTime() - before) / (double) TimeUnit.MILLISECONDS.toNanos(1);
            payloadSize = serialized.array().length;
            before = System.nanoTime();
            User deserializedUser = (User) deserializer.deserialize(serialized);
            long deserializeNanos = System.nanoTime() - before;
            double deserializeMillis = deserializeNanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
            //log.info("Deserialized user: {}", deserializedUser);
            log.info("Payload {} bytes, serialize: {} ms, deserialize: {} ms", payloadSize, serializeMillis, deserializeMillis);
            serialize.add(Pair.of(serializeMillis, deserializeMillis));
        }

    }

    public User read(String payloadJSONFile, Schema schema) throws IOException {
        String payloadJSON = FileUtils.readFileToString(new File(payloadJSONFile), "UTF-8");
        JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, payloadJSON);
        SpecificDatumReader<User> reader = new SpecificDatumReader<>(User.class);
        return reader.read(user, decoder);
    }
}
