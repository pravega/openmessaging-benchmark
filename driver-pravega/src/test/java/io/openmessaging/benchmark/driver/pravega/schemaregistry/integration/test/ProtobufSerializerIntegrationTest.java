package io.openmessaging.benchmark.driver.pravega.schemaregistry.integration.test;

import io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver;
import io.openmessaging.benchmark.driver.pravega.PravegaStandaloneUtils;
import io.openmessaging.benchmark.driver.pravega.ProtobufUtils;
import io.openmessaging.benchmark.driver.pravega.config.PravegaConfig;
import io.openmessaging.benchmark.driver.pravega.config.SchemaRegistryConfig;
import io.openmessaging.benchmark.driver.pravega.schema.generated.protobuf.Protobuf;
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.protobuf.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.server.rest.RestServer;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;
import io.pravega.test.common.TestUtils;
import lombok.extern.slf4j.Slf4j;
import io.pravega.schemaregistry.storage.SchemaStore;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.google.protobuf.util.JsonFormat;
import org.junit.rules.Timeout;

@Slf4j
public class ProtobufSerializerIntegrationTest implements AutoCloseable {

    private final static String resourcesPath = "src/test/resources/schema-registry/protobuf";

    private final Serializer<Protobuf.PBUser> serializer;
    private final Serializer<Protobuf.PBUser> deserializer;

    private final Protobuf.PBAddressEntry address = Protobuf.PBAddressEntry.newBuilder().setCity("Perth")
            .setStreetAddress("Avenue ").setPostalCode(40191).build();

    private final ScheduledExecutorService executor;
    private final RestServer restServer;
    @Rule
    public Timeout globalTimeout = new Timeout(3, TimeUnit.MINUTES);

    public ProtobufSerializerIntegrationTest() throws IOException {
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
                .createGroup(SerializationFormat.Protobuf, Compatibility.allowAny(), true).registerSchema(true)
                .build();

        log.info("serializerConfig: {}", serializerConfig);

        ProtobufSchema<Protobuf.PBUser> schema = ProtobufSchema.of(Protobuf.PBUser.class);
        serializer = SerializerFactory.protobufSerializer(serializerConfig, schema);
        deserializer = SerializerFactory.protobufDeserializer(serializerConfig, schema);
    }

    @Test
    public void testSerializer() throws IOException {
        Assert.assertEquals("Serialized to 100b",100, serializeProtobuf("/payload/pbuser-payload-100.json", 1));
    }

    @Test
    public void testGeneratePayload() throws IOException, URISyntaxException, NoSuchAlgorithmException {
        //100 bytes
        checkGeneratedUser(100, serializer, 0, 0, 0, 0, 0, 0, false);
        //1000 bytes
        checkGeneratedUser(1000, serializer, 109, 4, 0, 0, 0, 0,false);
        // 10000 bytes
        checkGeneratedUser(10000, serializer, 120, 40, 7, 1, 0, 0, false);
        // 100000 bytes
        checkGeneratedUser(100000, serializer, 100, 400, 299, 28, 49, 1, false);
        // 1000,000 bytes
        checkGeneratedUser(1000000, serializer, 1000, 497, 200, 3, 47, 2, false);
    }


    @Test
    public void testPayloadSize() throws IOException, URISyntaxException {
        Assert.assertEquals("Payload size should be 100b",100,
                serializeProtobuf( "/payload/pbuser-payload-100.json",
                        1));
        Assert.assertEquals("Payload size should be 1000b",1000,
                serializeProtobuf( "/payload/pbuser-payload-1000.json",
                        1));
        Assert.assertEquals("Payload size should be 10.000b",10000,
                serializeProtobuf( "/payload/pbuser-payload-10000.json",
                        1));
        Assert.assertEquals("Payload size should be 100.000b",100000,
                serializeProtobuf("/payload/pbuser-payload-100000.json",
                        1));
        Assert.assertEquals("Payload size should be 1.000.000b",1000000,
                serializeProtobuf( "/payload/pbuser-payload-1000000.json",
                        1));
    }

    @Test
    public void testSetEventTimestamp() {
        final Protobuf.PBUser user = Protobuf.PBUser.newBuilder()
                .setName("Greg Igan")
                .setAge(59)
                .setBiography("Australian science fiction writer and mathematician")
                .addAddress(0, address)
                .setEventTimestampInner(System.currentTimeMillis()).build();

        Protobuf.PBUser pbUser = Protobuf.PBUser.newBuilder(user).setEventTimestampInner(1).build();
        Assert.assertEquals(1L, pbUser.getEventTimestampInner());
        Assert.assertEquals(1, pbUser.getEventTimestamp().longValue());
        pbUser.setEventTimestamp(3L);
        Assert.assertEquals(3L, pbUser.getEventTimestampInner());
        Assert.assertEquals(3, pbUser.getEventTimestamp().longValue());
        ByteBuffer serialized = serializer.serialize(pbUser);
        Protobuf.PBUser deserializedUser = deserializer.deserialize(serialized);
        Assert.assertEquals(3L, deserializedUser.getEventTimestampInner());
        Assert.assertEquals(3, deserializedUser.getEventTimestamp().longValue());
    }

    private void checkGeneratedUser(int messageSize, Serializer<Protobuf.PBUser> serializer, int valueSize, int values, int valueSize2, int values2, int valueSize3, int values3, boolean writeToFile) throws IOException {
        Pair<Protobuf.PBUser, Integer> generated = ProtobufUtils.INSTANCE.innerGenerateUser(serializer, valueSize, values, valueSize2, values2, valueSize3, values3);
        Protobuf.PBUser user = generated.getLeft();
        int payloadSize = generated.getRight();
        if (writeToFile) {
            writePBUser( "/payload/generated/pbuser-payload-" + payloadSize + ".data", user);
        }
        Assert.assertEquals(String.format("Serialized to %db", messageSize), messageSize, payloadSize);
    }


    private void writePBUser(String payloadFile, Protobuf.PBUser user) throws IOException {
        String userJson = JsonFormat.printer().print(user);
        FileUtils.writeStringToFile(new File(resourcesPath + payloadFile), userJson, StandardCharsets.UTF_8);
    }

    private int serializeProtobuf(String payloadFile, int serializeTimes) throws IOException {
        log.info("Payload file: {}", payloadFile);
        String userJson = FileUtils.readFileToString(new File(resourcesPath + payloadFile), "UTF-8");
        Protobuf.PBUser.Builder builder = Protobuf.PBUser.newBuilder();
        JsonFormat.parser().merge(userJson, builder);
        Protobuf.PBUser user = builder.build();
        List<Pair<Double, Double>> serialize = new LinkedList<>();
        int payloadSize = 0;
        Protobuf.PBUser deserializedUser = null;
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
        //log.info("Serialized user: {}", new String(serialized.array(), StandardCharsets.UTF_8));
        //log.info("Deserialized user: {}", deserializedUser);
        return payloadSize;
    }

    @After
    @Override
    public void close()  {
        restServer.stopAsync();
        restServer.awaitTerminated();
        executor.shutdownNow();
    }
}
