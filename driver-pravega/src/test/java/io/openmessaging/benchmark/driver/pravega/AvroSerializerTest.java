package io.openmessaging.benchmark.driver.pravega;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
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
        serializeAvro("src/test/resources/schema-registry/pravega.yaml", "src/test/resources/schema-registry/user-payload-100b.json", 10);
    }

    @Test
    public void testDefaultAvroSerializer() throws IOException {
        int serializeTimes = 10;
        String payloadFile = "src/test/resources/schema-registry/payload-100b.data";
        String schemaFile = "src/test/resources/schema-registry/user.avsc";
        ObjectMapper mapper = new ObjectMapper();
        User user = mapper.readValue(new File(payloadFile), User.class);
        Schema schema;
        try {
            schema = new Schema.Parser().parse(new File(schemaFile));
        } catch (IOException e) {
            log.error("Schema {} is invalid.", schemaFile, e);
            throw e;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        BinaryDecoder decoder;
        DatumWriter<User> datumWriter = new SpecificDatumWriter(schema);
        SpecificDatumReader<User> reader = new SpecificDatumReader(schema);
        int payloadSize;
        try {
            for (int i = 0; i < serializeTimes; i++) {
                // serialize
                long before = System.nanoTime();
                datumWriter.write(user, encoder);
                encoder.flush();
                byte[] serialized = out.toByteArray();
                long serializeNanos = System.nanoTime() - before;
                double serializeMillis = serializeNanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
                payloadSize = serialized.length;
                // deserialize
                before = System.nanoTime();
                decoder = DecoderFactory.get().binaryDecoder(serialized, null);
                User deserialized = reader.read(null, decoder);
                long deserializeNanos = System.nanoTime() - before;
                double deserializeMillis = deserializeNanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
                log.info("Payload {} bytes, serialize: {}ms, deserialize: {} ms", payloadSize, serializeMillis, deserializeMillis);
                out.reset();
            }
        } finally {
            out.close();
        }
    }

    @Test
    public void testDefaultSerializer() throws IOException {
        serializeByte("src/test/resources/schema-registry/payload-100b.data", 10);
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
            log.info("Payload {} bytes, serialize: {}ms, deserialize: {}ms", payloadSize, serializeMillis, deserializeMillis);
            serialize.add(Pair.of(serializeMillis, deserializeMillis));
        }

    }
}
