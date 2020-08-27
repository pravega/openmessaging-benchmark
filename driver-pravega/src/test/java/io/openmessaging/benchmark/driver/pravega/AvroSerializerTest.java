package io.openmessaging.benchmark.driver.pravega;

import com.fasterxml.jackson.databind.ObjectMapper;
//import com.linkedin.avro.fastserde.FastSpecificDatumReader;
//import com.linkedin.avro.fastserde.FastSpecificDatumWriter;
//import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import io.openmessaging.benchmark.driver.pravega.config.PravegaConfig;
import io.openmessaging.benchmark.driver.pravega.config.SchemaRegistryConfig;
import io.openmessaging.benchmark.driver.pravega.testobj.User;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.nio.file.Files.readAllBytes;

@Slf4j
public class AvroSerializerTest {

    private ByteBuffer timestampAndPayload;

    //@Ignore //TODO make unit test, not requiring standalone pravega running along with registry
    @Test
    public void testWithStandalonePravega() throws IOException, URISyntaxException {
        serializeAvro("src/test/resources/schema-registry/pravega-standalone.yaml", "src/test/resources/schema-registry/user-payload-100b.json", 10);
    }

    @Test
    public void testSerializeAvro() throws IOException, URISyntaxException {
        serializeAvro("src/test/resources/schema-registry/pravega.yaml", "src/test/resources/schema-registry/user-payload-100b.json", 100);
    }

    @Test
    public void test() {
        org.glassfish.jersey.internal.LocalizationMessages.WARNING_PROPERTIES();
    }

    @Test
    public void testDefaultAvroSerializer() throws IOException {
        int serializeTimes = 10;
        String payloadFile = "src/test/resources/schema-registry/user-payload-100b.json";
        String schemaFile = "src/test/resources/schema-registry/user.avsc";
        ObjectMapper mapper = new ObjectMapper();
        User user = mapper.readValue(new File(payloadFile), User.class);
        Schema avroschema;
        try {
            avroschema = new Schema.Parser().parse(new File(schemaFile));
        } catch (IOException e) {
            log.error("Schema {} is invalid.", schemaFile, e);
            throw e;
        }
        User newUser;
        int payloadSize;
        for (int i = 0; i < serializeTimes; i++) {
            // serialize
            long before = System.nanoTime();
            byte[] serialized = toBytesGeneric(user, avroschema);
            long serializeNanos = System.nanoTime() - before;
            double serializeMillis = serializeNanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
            payloadSize = serialized.length;
            // deserialize
            before = System.nanoTime();
            newUser = fromBytesGeneric(serialized, avroschema, user);
            long deserializeNanos = System.nanoTime() - before;
            double deserializeMillis = deserializeNanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
            log.info("Payload {} bytes, serialize: {}ms, deserialize: {}ms", payloadSize, serializeMillis, deserializeMillis);
        }
    }

//    @Test
//    public void testFastAvroSerializer() throws Exception {
//        int serializeTimes = 10;
//        //String payloadFile = "src/test/resources/schema-registry/user-payload-100b.json";
//        String line = Files.readAllLines(Paths.get("src/test/resources/schema-registry/user-oneline-100b.json")).get(0);
//        String schemaFile = "src/test/resources/schema-registry/user-light.avsc";
//        Schema avroschema;
//        try {
//            avroschema = new Schema.Parser().parse(new File(schemaFile));
//        } catch (IOException e) {
//            log.error("Schema {} is invalid.", schemaFile, e);
//            throw e;
//        }
//        GenericRecord user, newUser;
//        user = jsonToAvro(line, avroschema);
//
//        int payloadSize;
//        for (int i = 0; i < serializeTimes; i++) {
//            // serialize
//            long before = System.nanoTime();
//            byte[] serialized = fast2BytesGeneric(user, avroschema);
//            long serializeNanos = System.nanoTime() - before;
//            double serializeMillis = serializeNanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
//            payloadSize = serialized.length;
//            // deserialize
////            before = System.nanoTime();
////            newUser = fastFromBytesGeneric(serialized, avroschema, null);
////            long deserializeNanos = System.nanoTime() - before;
////            double deserializeMillis = deserializeNanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
//            log.info("Payload {} bytes, serialize: {}ms, deserialize: ms", payloadSize, serializeMillis);//, deserializeMillis);
//        }
//    }

    private static Object jsonToAvro(InputStream stream, Schema schema) throws Exception {
        DatumReader<Object> reader = new GenericDatumReader<>(schema);
        Object object = reader.read(null, DecoderFactory.get().jsonDecoder(schema, stream));

        if (schema.getType().equals(Schema.Type.STRING)) {
            object = object.toString();
        }
        return object;
    }

    private static GenericRecord jsonToAvro(String input, Schema schema) throws Exception {
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        GenericRecord object = reader.read(null, DecoderFactory.get().jsonDecoder(schema, input));

//        if (schema.getType().equals(Schema.Type.STRING)) {
//            object = object.toString();
//        }
        return object;
    }

//    public static <V> byte[] fast2BytesGeneric(final V v, final Schema schema) {
//        final ByteArrayOutputStream bout = new ByteArrayOutputStream();
//        final DatumWriter<V> writer = new FastSpecificDatumWriter<V>(schema);
//        final BinaryEncoder binEncoder = AvroCompatibilityHelper.newBinaryEncoder(bout, false, null);
//        try {
//            writer.write(v, binEncoder);
//            binEncoder.flush();
//        } catch (final Exception e) {
//            throw new RuntimeException(e);
//        }
//        return bout.toByteArray();
//    }
//
//    public static <V> V fastFromBytesGeneric(final byte[] serialized, final Schema schema, final V oldV) {
//        final DatumReader<V> reader = new FastSpecificDatumReader<V>(schema);
//        final BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(serialized);
//        try {
//            return reader.read(oldV, decoder);
//        } catch (final Exception e) {
//            throw new RuntimeException(e);
//        }
//    }

    public static <V> byte[] toBytesGeneric(final V v, final Schema schema) {
        final ByteArrayOutputStream bout = new ByteArrayOutputStream();
        final DatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
        final BinaryEncoder binEncoder = EncoderFactory.get().binaryEncoder(bout, null);
        try {
            writer.write(v, binEncoder);
            binEncoder.flush();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return bout.toByteArray();
    }

    public static <V> V fromBytesGeneric(final byte[] serialized, final Schema schema, final V oldV) {
        final DatumReader<V> reader = new ReflectDatumReader<V>(schema);
        final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(serialized, null);
        try {
            return reader.read(oldV, decoder);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDefaultSerializer() throws IOException {
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
        AvroSchema<Object> schema;
        try {
            schema = AvroSchema.of(new Schema.Parser().parse(new File(schemaRegistryConfig.schemaFile)));
        } catch (IOException e) {
            log.error("Schema {} is invalid.", schemaRegistryConfig.schemaFile, e);
            throw e;
        }
        Serializer<Object> serializer = SerializerFactory
                .avroSerializer(serializerConfig, schema);
        Serializer<Object> deserializer = SerializerFactory.avroDeserializer(
                serializerConfig, schema);

        ObjectMapper mapper = new ObjectMapper();
        User user = mapper.readValue(new File(payloadFile), User.class);
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
}
