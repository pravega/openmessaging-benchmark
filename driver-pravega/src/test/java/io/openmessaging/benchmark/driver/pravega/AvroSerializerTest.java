package io.openmessaging.benchmark.driver.pravega;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openmessaging.benchmark.driver.pravega.config.PravegaConfig;
import io.openmessaging.benchmark.driver.pravega.config.SchemaRegistryConfig;
import io.openmessaging.benchmark.driver.pravega.testobj.User;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AvroSerializerTest {

    //@Ignore //TODO make unit test, not requiring standalone pravega running along with registry
    @Test
    public void testWithStandalonePravega() throws IOException, URISyntaxException {
        runTest("src/test/resources/schema-registry/pravega-standalone.yaml", "src/test/resources/schema-registry/user-payload-100b.json", 10);
    }

    @Test
    public void test() throws IOException, URISyntaxException {
        runTest("src/test/resources/schema-registry/pravega.yaml", "src/test/resources/schema-registry/user-payload-100b.json", 10);
    }

    private void runTest(String driverConfigFile, String payloadFile, int serializeTimes) throws IOException, URISyntaxException {
        File driverfile = new File(driverConfigFile);
        PravegaConfig driverConfig = PravegaBenchmarkDriver.readConfig(driverfile);
        SchemaRegistryConfig schemaRegistryConfig = driverConfig.schemaRegistry;
        log.info("schema file: {}", schemaRegistryConfig.schemaFile);
        SchemaRegistryClientConfig config = null;
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
        AvroSchema<Object> schema = null;
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
        int payloadSize = 0;
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
            log.info("Payload {} bytes, serialize: {}ms, deserialize: {}ms", payloadSize, deserializeMillis, deserializeMillis);
            serialize.add(Pair.of(serializeMillis, deserializeMillis));
        }

    }
}
