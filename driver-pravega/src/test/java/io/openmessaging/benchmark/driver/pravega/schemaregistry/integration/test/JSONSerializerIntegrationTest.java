package io.openmessaging.benchmark.driver.pravega.schemaregistry.integration.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openmessaging.benchmark.driver.pravega.JSONUtils;
import io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver;
import io.openmessaging.benchmark.driver.pravega.PravegaStandaloneUtils;
import io.openmessaging.benchmark.driver.pravega.config.PravegaConfig;
import io.openmessaging.benchmark.driver.pravega.config.SchemaRegistryConfig;
import io.openmessaging.benchmark.driver.pravega.schema.generated.protobuf.Protobuf;
import io.openmessaging.benchmark.driver.pravega.schema.json.JSONUser;
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.protobuf.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.server.rest.RestServer;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;
import io.pravega.test.common.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.*;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JSONSerializerIntegrationTest implements AutoCloseable {

    private final static String resourcesPath = "src/test/resources/schema-registry/json";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Serializer<JSONUser> serializer, deserializer;
    private final RestServer restServer;
    private final ScheduledExecutorService executor;
    @Rule
    public Timeout globalTimeout = new Timeout(3, TimeUnit.MINUTES);

    public JSONSerializerIntegrationTest() throws IOException {
        PravegaStandaloneUtils pravegaStandaloneUtils = PravegaStandaloneUtils.startPravega();
        executor = Executors.newScheduledThreadPool(10);

        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create(pravegaStandaloneUtils.getControllerURI())).build();

        SchemaStore schemaStore = SchemaStoreFactory.createPravegaStore(clientConfig, executor);

        SchemaRegistryService service = new SchemaRegistryService(schemaStore, executor);
        int port = TestUtils.getAvailableListenPort();
        ServiceConfig serviceConfig = ServiceConfig.builder().port(port).build();

        restServer = new RestServer(service, serviceConfig);
        restServer.startAsync();
        restServer.awaitRunning();

        String driverConfigFile = resourcesPath + "/pravega-standalone.yaml";
        File driverfile = new File(driverConfigFile);
        PravegaConfig driverConfig = PravegaBenchmarkDriver.readConfig(driverfile);
        SchemaRegistryConfig schemaRegistryConfig = driverConfig.schemaRegistry;

        String schemaRegistryURI = "http://localhost:" + port;
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create(schemaRegistryURI)).build();

        // Create serializer and deserializer
        SerializerConfig serializerConfig = SerializerConfig.builder()
                .groupId(schemaRegistryConfig.groupId).registryConfig(config)
                .createGroup(SerializationFormat.Json, Compatibility.allowAny(), true).registerSchema(true)
                .build();

        log.info("serializerConfig: {}", serializerConfig);

        final JSONSchema<JSONUser> schema = JSONSchema.of(JSONUser.class);
        serializer = SerializerFactory.jsonSerializer(serializerConfig, schema);
        deserializer = SerializerFactory.jsonDeserializer(serializerConfig, schema);
    }

    @Test
    public void testGenerateJSON() throws IOException, URISyntaxException, NoSuchAlgorithmException {
        //100 bytes
        checkGeneratedUser(100, serializer, 0, 0, 0, 0, 0, 0, false);
        //1000 bytes
        checkGeneratedUser(1002, serializer, 85, 3, 62, 2, 0, 0, false);
        // 10007 bytes
        checkGeneratedUser(10003, serializer, 100, 24, 100, 19, 56, 2, false);
        // 100000 bytes
        checkGeneratedUser(100048, serializer, 100, 240, 100, 183, 100, 25, false);
        // 1000,000 bytes
        checkGeneratedUser(999914, serializer, 500, 315, 500, 301, 499, 362, false);
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

    @Test
    public void testPayloadSize() throws IOException, URISyntaxException {
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

    @After
    @Override
    public void close()  {
        restServer.stopAsync();
        restServer.awaitTerminated();
        executor.shutdownNow();
    }

}
