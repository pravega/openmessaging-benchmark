package io.openmessaging.benchmark.driver.pravega.schemaregistry.integration.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openmessaging.benchmark.driver.pravega.JSONUtils;
import io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver;
import io.openmessaging.benchmark.driver.pravega.ProtobufUtils;
import io.openmessaging.benchmark.driver.pravega.config.PravegaConfig;
import io.openmessaging.benchmark.driver.pravega.config.SchemaRegistryConfig;
import io.openmessaging.benchmark.driver.pravega.schema.generated.protobuf.Protobuf;
import io.openmessaging.benchmark.driver.pravega.schema.json.JSONUser;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.protobuf.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
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
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.google.protobuf.util.JsonFormat;

@Slf4j
@Ignore
public class ProtobufSerializerIntegrationTest {

    private final static String resourcesPath = "src/test/resources/schema-registry/protobuf";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ProtobufSchema<Protobuf.PBUser> schema = ProtobufSchema.of(Protobuf.PBUser.class);

    private final String driverConfigFile = resourcesPath + "/pravega-standalone.yaml";
    private Serializer<Protobuf.PBUser> serializer, deserializer;

    private final Protobuf.PBAddressEntry address = Protobuf.PBAddressEntry.newBuilder().setCity("Perth")
            .setStreetAddress("Avenue ").setPostalCode(40191).build();

    private final Protobuf.PBUser user = Protobuf.PBUser.newBuilder()
            .setName("Greg Igan")
            .setAge(59)
            .setBiography("Australian science fiction writer and mathematician")
            .addAddress(0, address)
            .setEventTimestampInner(System.currentTimeMillis()).build();

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
                .createGroup(SerializationFormat.Protobuf, Compatibility.allowAny(), true).registerSchema(true)
                .build();

        log.info("serializerConfig: {}", serializerConfig);

        serializer = SerializerFactory.protobufSerializer(serializerConfig, schema);
        deserializer = SerializerFactory.protobufDeserializer(serializerConfig, schema);
    }

    @Test
    public void test() throws IOException {
        Assert.assertEquals("Serialized to 100b",100, serializeProtobuf("/payload/pbuser-payload-100.json", 1));
    }

    @Ignore
    @Test
    public void testGenerateJSON() throws IOException, URISyntaxException, NoSuchAlgorithmException {
        //100 bytes
        //checkGeneratedUser(100, serializer, 0, 0, 0, 0, 0, 0, false);
        //1000 bytes
        //checkGeneratedUser(1000, serializer, 109, 4, 0, 0, 0, 0, true);
        // 10007 bytes
        //checkGeneratedUser(10000, serializer, 120, 40, 7, 1, 0, 0, true);
        // 100000 bytes
        //checkGeneratedUser(100000, serializer, 100, 400, 299, 28, 49, 1, true);
        // 1000,000 bytes
        checkGeneratedUser(1000000, serializer, 1000, 497, 200, 3, 47, 2, true);
    }

    @Ignore
    @Test
    public void testWithStandalonePravega() throws IOException, URISyntaxException {
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
}
