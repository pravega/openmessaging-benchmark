package io.openmessaging.benchmark.driver.pravega.config;

/**
 * Refer to io.pravega.schemaregistry.contract.data.VersionInfo
 */
public class VersionInfo {
    /**
     * Object type which is declared in the corresponding type for the schemainfo that is identified
     * by this version info.
     */
    public String type;
    /**
     * A version number that identifies the position of schema among other schemas in the group that share the same 'type'.
     */
    public int version;
    /**
     * A position identifier that uniquely identifies the schema within a group and represents the order in which this
     * schema was included in the group.
     */
    public int id;
}
