package io.openmessaging.benchmark.driver.pravega;

import com.google.common.math.IntMath;
import io.openmessaging.benchmark.driver.pravega.config.PravegaConfig;
import io.openmessaging.benchmark.driver.pravega.config.SchemaRegistryConfig;
import io.openmessaging.benchmark.driver.pravega.schema.generated.avro.*;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.RandomData;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.apache.commons.text.RandomStringGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.nio.file.Files.readAllBytes;


//TODO make unit test, not requiring standalone pravega running along with registry
// TODO fix warnings
@Slf4j
public class AvroSerializerTest {

    private ByteBuffer timestampAndPayload;
    private final static String resourcesPath = "src/test/resources/schema-registry/avro";
    private final String schemaFile =  resourcesPath + "/user.avsc";
    private User user;
    private User user1000;
    private User user1000B;
    private Schema schema;
    SecureRandom number = new SecureRandom();

    @Before
    public void init() throws IOException {
        try {
            schema = new Schema.Parser().parse(new File(schemaFile));
            Schema builtSchema = SchemaBuilder.builder("io.openmessaging.benchmark.driver.pravega.schema.generated.avro")
                    .record("User")
                    .fields()
                    .endRecord();

            user = User.newBuilder()
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

            user1000 = User.newBuilder()
                    .setUserId("Iu6AxdBYGR4A0wspR9BYHA")
                    .setBiography("Greg Egan was born 20 August 1961. Egan is an Australian science fiction writer and amateur mathematician, best known for his works of hard science fiction. Egan has won multiple awards including the John W. Campbell Memorial Award, the Hugo Award, and the Locus Award. Egan holds a Bachelor of Science degree in Mathematics from the University of Western Australia.[3][4][5]\n" +
                            "\n" +
                            "He published his first work in 1983.[6] He specialises in hard science fiction stories with mathematical and quantum ontology themes, including the nature of consciousness. Other themes include genetics, simulated reality, posthumanism, mind uploading, sexuality, artificial intelligence, and the superiority of rational naturalism to religion. He often deals with complex technical material, like new physics and epistemology. He is a Hugo Award winner (with eight other works shortlisted for the Hugos) and has also won the John W. Campbell Award ...")
                    .setEventTimestamp(System.currentTimeMillis())
                    .setName("Greg Egan")
                    .setKeyValues(null)
                    .setKeyValues2(null)
                    .setKeyValues3(null)
                    .setAddress(AddressEntry.newBuilder().setCity("Perth")
                            .setPostalCode(5018)
                            .setStreetAddress("4/19 Gardner Road").build()).build();

            user1000B = User.newBuilder()
                    .setUserId("Iu6AxdBYGR4A0wspR9BYHA")
                    .setBiography("Greg Egan was born 20 August 1961. Egan is an Australian science fiction writer and amateur mathematician, best known for his works of hard science fiction. Egan has won multiple awards including the John W. Campbell Memorial Award, the Hugo Award, and the Locus Award. Egan holds a Bachelor of Science degree in Mathematics from the University of Western Australia.[3][4][5]\n")
                    .setEventTimestamp(System.currentTimeMillis())
                    .setName("Greg Egan")
                    .setKeyValues(null)
                    .setKeyValues2(null)
                    .setKeyValues3(null)
                    .setKeyValues(Collections.singletonList(KeyValue.newBuilder().setName("Name").setValue("Greg Egan").build()))
                    .setAddress(AddressEntry.newBuilder().setCity("Perth")
                            .setPostalCode(5018)
                            .setStreetAddress("4/19 Gardner Road").build()).build();

        } catch (IOException e) {
            log.error("Schema {} is invalid", schemaFile);
            throw e;
        }
    }

    @Test
    public void generateJSONFromSchema() throws IOException {
        Iterator<Object> it = new RandomData(schema, 1).iterator();
        Object generated = it.next();
        System.out.println(generated);
        FileOutputStream fout = new FileOutputStream(resourcesPath + "/payload/user-generated.json");
        Encoder encoder = EncoderFactory.get().jsonEncoder(schema, fout, true);
        DatumWriter writer = new SpecificDatumWriter<>(schema);
        writer.write(generated, encoder);
        encoder.flush();
        fout.close();

    }

    @Test
    public void testSerializeToJson() throws IOException {
        FileOutputStream fout = new FileOutputStream(resourcesPath + "/payload/user-1000B.json");
        Encoder encoder = EncoderFactory.get().jsonEncoder(schema, fout, true);
        DatumWriter<User> writer = new SpecificDatumWriter<>(User.class);
        writer.write(user1000B, encoder);
        encoder.flush();
        fout.close();
    }

    @Ignore
    @Test
    public void testWithStandalonePravega() throws IOException, URISyntaxException {
        Assert.assertEquals("Payload size should be 100b",100,
                serializeAvro(resourcesPath + "/pravega-standalone.yaml",
                        resourcesPath + "/payload/user-payload-100.json",
                        1));
        Assert.assertEquals("Payload size should be 1000b",1000,
                serializeAvro(resourcesPath + "/pravega-standalone.yaml",
                        resourcesPath + "/payload/user-payload-1000.json",
                        1));
        Assert.assertEquals("Payload size should be 10.000b",10000,
                serializeAvro(resourcesPath + "/pravega-standalone.yaml",
                        resourcesPath + "/payload/user-payload-10000.json",
                        1));
        Assert.assertEquals("Payload size should be 100.000b",100000,
                serializeAvro(resourcesPath + "/pravega-standalone.yaml",
                        resourcesPath + "/payload/user-payload-100000.json",
                        1));
        Assert.assertEquals("Payload size should be 1.000.000b",1000000,
                serializeAvro(resourcesPath + "/pravega-standalone.yaml",
                        resourcesPath + "/payload/user-payload-1000000.json",
                        1));
    }

    @Ignore
    @Test
    public void testSerializeAvro() throws IOException, URISyntaxException {
        serializeAvro(resourcesPath + "/pravega.yaml", resourcesPath + "/payload/user-payload-100b.json", 100);
    }

    public String generateRandomString(int size) throws NoSuchAlgorithmException {
        SecureRandom number = SecureRandom.getInstanceStrong();
        byte[] array = number.generateSeed(size);
        return new String(array, StandardCharsets.UTF_8);
    }

    public String generateRandomString2(int size)  {
        UniformRandomProvider rng = RandomSource.create(RandomSource.MT);
        RandomStringGenerator generator = new RandomStringGenerator.Builder().withinRange('A', 'z').usingRandom(rng::nextInt).build();
        String s = generator.generate(size);
        int payload = s.getBytes(StandardCharsets.UTF_8).length;
        Assert.assertEquals(size, payload);
        return s;
    }

    @Ignore
    @Test
    public void testGenerateJSON() throws IOException, URISyntaxException, NoSuchAlgorithmException {
        File driverfile = new File(resourcesPath + "/pravega-standalone.yaml");
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
                .createGroup(SerializationFormat.Avro).registerSchema(true)
                .build();
        AvroSchema<User> avroSchema = AvroSchema.of(User.class);
        Serializer<User> serializer = SerializerFactory
                .avroSerializer(serializerConfig, avroSchema);


        // ~10000 bytes
        //generateUser(10000, serializer, 125, 10, 10, 12);
        // 10005 bytes
        //generateUser(100000, serializer, 500, 99, 100, 2, 11, 4);
        // 100,000 bytes
        generateUser(1000000, serializer, 1000, 301, 500, 400, 250, 104);
    }


    // Will work only for incompressable strings
    public void generateJSON(int messageSize, Serializer serializer) throws NoSuchAlgorithmException, IOException, URISyntaxException {
        // {key:69b value:69b} keyValue string = +500 serialized bytes
        int inputBytes = 100;
        int serialized = 500;

        int values = IntMath.divide(messageSize, serialized, RoundingMode.FLOOR);
        int left = messageSize - values * serialized;
        log.info("size {} values:{} left: {}", values * serialized, values, left);
        generateUser(messageSize, serializer, inputBytes, values, 0, 0, 0, 0);
    }

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

    @Test
    public void testDefaultAvroSerializer() {
        serializeAvro(1, user1000);
    }

    public void serializeAvro(int serializeTimes, User user) {
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
        serializeByte(resourcesPath + "/payload-100b.data", 100);
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

    private int serializeAvro(String driverConfigFile, String payloadFile, int serializeTimes) throws IOException, URISyntaxException {
        File driverfile = new File(driverConfigFile);
        PravegaConfig driverConfig = PravegaBenchmarkDriver.readConfig(driverfile);
        SchemaRegistryConfig schemaRegistryConfig = driverConfig.schemaRegistry;
        log.info("schema file: {} payload file: {}", schemaRegistryConfig.schemaFile, payloadFile);
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

    public User read(String payloadJSONFile, Schema schema) throws IOException {
        String payloadJSON = FileUtils.readFileToString(new File(payloadJSONFile), "UTF-8");
        JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, payloadJSON);
        SpecificDatumReader<User> reader = new SpecificDatumReader<>(User.class);
        return reader.read(user, decoder);
    }
}
