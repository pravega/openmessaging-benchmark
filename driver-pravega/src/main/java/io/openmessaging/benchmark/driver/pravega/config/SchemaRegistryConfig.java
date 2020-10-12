package io.openmessaging.benchmark.driver.pravega.config;

import io.pravega.schemaregistry.contract.data.SerializationFormat;

public class SchemaRegistryConfig {

    public String schemaRegistryURI;
    public String groupId;
    public VersionInfo version;
    public SerializationFormat serializationFormat;
    /**
     * Required for protobuf and avro formats
     */
    public String schemaFile;
}
