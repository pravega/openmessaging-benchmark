package io.openmessaging.benchmark.driver.pravega.schemaregistry.integration.test;

import io.openmessaging.benchmark.driver.pravega.AvroUtils;
import io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver;
import io.openmessaging.benchmark.driver.pravega.config.PravegaConfig;
import io.openmessaging.benchmark.driver.pravega.config.SchemaRegistryConfig;
import io.openmessaging.benchmark.driver.pravega.schema.generated.avro.AddressEntry;
import io.openmessaging.benchmark.driver.pravega.schema.generated.avro.KeyValue;
import io.openmessaging.benchmark.driver.pravega.schema.generated.avro.User;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.RandomData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Iterator;



@Slf4j
@Ignore
public class AvroSerializerIntegrationTest {

    private final static String resourcesPath = "src/test/resources/schema-registry/avro";

    private final User user1000 = User.newBuilder()
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

    private final User user1000B = User.newBuilder()
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
    private Schema schema;
    private Serializer<User> serializer, deserializer;

    @Before
    public void init() throws IOException, URISyntaxException {
        String schemaFile = resourcesPath + "/user.avsc";
        try {
            schema = new Schema.Parser().parse(new File(schemaFile));
        } catch (IOException e) {
            log.error("Schema {} is invalid", schemaFile);
            throw e;
        }
        String driverConfigFile = resourcesPath + "/pravega-standalone.yaml";
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
                .createGroup(SerializationFormat.Avro).registerSchema(true)
                .build();
        AvroSchema<User> schema = AvroSchema.of(User.class);
        this.serializer = SerializerFactory
                .avroSerializer(serializerConfig, schema);
        this.deserializer = SerializerFactory.avroDeserializer(
                serializerConfig, schema);
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
                serializeAvroWithStandalonePravega(
                        resourcesPath + "/payload/user-payload-100.json",
                        1));
        Assert.assertEquals("Payload size should be 1000b",1000,
                serializeAvroWithStandalonePravega(
                        resourcesPath + "/payload/user-payload-1000.json",
                        1));
        Assert.assertEquals("Payload size should be 10.000b",10000,
                serializeAvroWithStandalonePravega(
                        resourcesPath + "/payload/user-payload-10000.json",
                        1));
        Assert.assertEquals("Payload size should be 100.000b",100000,
                serializeAvroWithStandalonePravega(
                        resourcesPath + "/payload/user-payload-100000.json",
                        1));
        Assert.assertEquals("Payload size should be 1.000.000b",1000000,
                serializeAvroWithStandalonePravega(
                        resourcesPath + "/payload/user-payload-1000000.json",
                        1));
    }

    public int serializeAvroWithStandalonePravega(String payloadFile, int serializeTimes) throws IOException{
        return AvroUtils.INSTANCE.serializeAvro(payloadFile, schema, serializer, deserializer , serializeTimes);
    }

    @Ignore
    @Test
    public void testGenerateJSON() throws IOException, NoSuchAlgorithmException {
        // ~10000 bytes
        //generateUser(10000, serializer, 125, 10, 10, 12);
        // 10005 bytes
        //generateUser(100000, serializer, 500, 99, 100, 2, 11, 4);
        // 100,000 bytes
        AvroUtils.INSTANCE.generateUser(1000000, serializer, 1000, 301, 500, 400, 250, 104);
    }

    @Test
    public void testDefaultAvroSerializer() {
        AvroUtils.INSTANCE.serializeAvroGeneric(1, user1000);
    }

    @Test
    public void testByteSerializer() throws IOException {
        AvroUtils.INSTANCE.serializeByte(resourcesPath + "/payload/payload-100b.data", 100);
    }

}
