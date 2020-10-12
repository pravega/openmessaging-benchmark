package io.openmessaging.benchmark.driver.pravega.schemaregistry.integration.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openmessaging.benchmark.driver.pravega.JSONUtils;
import io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver;
import io.openmessaging.benchmark.driver.pravega.config.PravegaConfig;
import io.openmessaging.benchmark.driver.pravega.config.SchemaRegistryConfig;
import io.openmessaging.benchmark.driver.pravega.schema.json.JSONUser;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Ignore
public class JSONSerializerIntegrationTest {

    private final static String resourcesPath = "src/test/resources/schema-registry/json";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final JSONSchema<JSONUser> schema = JSONSchema.of(JSONUser.class);

    private final String driverConfigFile = resourcesPath + "/pravega-standalone.yaml";
    private Serializer<JSONUser> serializer, deserializer;

    @Before
    public void prepareTest() throws IOException, URISyntaxException {
        File driverfile = new File(driverConfigFile);
        PravegaConfig driverConfig = PravegaBenchmarkDriver.readConfig(driverfile);
        SchemaRegistryConfig schemaRegistryConfig = driverConfig.schemaRegistry;

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
                .createGroup(SerializationFormat.Json, Compatibility.allowAny(), true).registerSchema(true)
                .build();

        log.info("serializerConfig: {}", serializerConfig);

        serializer = SerializerFactory.jsonSerializer(serializerConfig, schema);
        deserializer = SerializerFactory.jsonDeserializer(serializerConfig, schema);
    }

    @Ignore
    @Test
    public void testGenerateJSON() throws IOException, URISyntaxException, NoSuchAlgorithmException {
        //100 bytes
        //checkGeneratedUser(100, serializer, 0, 0, 0, 0, 0, 0, false);
        //1000 bytes
        //checkGeneratedUser(1000, serializer, 85, 3, 59, 2, 0, 0, true);
        // 10007 bytes
        //checkGeneratedUser(10007, serializer, 100, 24, 100, 19, 49, 1, true);
        // 100000 bytes
        //checkGeneratedUser(100000, serializer, 100, 240, 100, 180, 100, 21, true);
        // 1000,000 bytes
        //checkGeneratedUser(1000000, serializer, 500, 300, 500, 300, 499, 362, true);
    }

    private void checkGeneratedUser(int messageSize, Serializer<JSONUser> serializer, int valueSize, int values, int valueSize2, int values2, int valueSize3, int values3, boolean writeToFile) throws IOException {
        Pair<JSONUser, Integer> generated = JSONUtils.INSTANCE.innerGenerateUser(serializer, valueSize, values, valueSize2, values2, valueSize3, values3);
        JSONUser user = generated.getLeft();
        int payloadSize = generated.getRight();
        if (writeToFile) {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(resourcesPath + "/payload/generated/jsonuser-payload-" + payloadSize + ".data"), user);
        }
        Assert.assertEquals(String.format("Serialized to %db", messageSize), messageSize, payloadSize);
    }

    @Ignore
    @Test
    public void testWithStandalonePravega() throws IOException, URISyntaxException {
        Assert.assertEquals("Payload size should be 100b",100,
                serializeJSON(resourcesPath + "/payload/jsonuser-payload-100.json",
                        1));
        Assert.assertEquals("Payload size should be 1000b",1000,
                serializeJSON(resourcesPath + "/payload/jsonuser-payload-1000.json",
                        1));
        Assert.assertEquals("Payload size should be 10.000b",10000,
                serializeJSON(resourcesPath + "/payload/jsonuser-payload-10000.json",
                        1));
        Assert.assertEquals("Payload size should be 100.000b",100000,
                serializeJSON(resourcesPath + "/payload/jsonuser-payload-100000.json",
                        1));
        Assert.assertEquals("Payload size should be 1.000.000b",1000000,
                serializeJSON(resourcesPath + "/payload/jsonuser-payload-1000000.json",
                        1));
    }

    private int serializeJSON(String payloadFile, int serializeTimes) throws IOException, URISyntaxException {

        log.info("Payload file: {}", payloadFile);

        JSONUser user = read(payloadFile);
        user.setEventTimestamp(System.currentTimeMillis());
        List<Pair<Double, Double>> serialize = new LinkedList<>();
        int payloadSize = 0;
        JSONUser deserializedUser = null;
        ByteBuffer serialized;
        for (int i = 0; i < serializeTimes; i++) {
            long before = System.nanoTime();
            serialized = serializer.serialize(user);
            double serializeMillis = (System.nanoTime() - before) / (double) TimeUnit.MILLISECONDS.toNanos(1);
            payloadSize = serialized.limit();
            before = System.nanoTime();
            deserializedUser = deserializer.deserialize(serialized);
            long deserializeNanos = System.nanoTime() - before;
            double deserializeMillis = deserializeNanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
            //log.info("Deserialized user: {}", deserializedUser);
            log.info("Payload {} bytes, serialize: {} ms, deserialize: {} ms", payloadSize, serializeMillis, deserializeMillis);
            serialize.add(Pair.of(serializeMillis, deserializeMillis));
        }
        //log.info("Seserialized user: {}", new String(serialized.array(), StandardCharsets.UTF_8));
        //log.info("Deserialized user: {}", deserializedUser);
        return payloadSize;
    }

    public JSONUser read(String payloadJSONFile) throws IOException {
        JSONUser payload = objectMapper.readValue(new File(payloadJSONFile), JSONUser.class);
        return payload;
    }

}
