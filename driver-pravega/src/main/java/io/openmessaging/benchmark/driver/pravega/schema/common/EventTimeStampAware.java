package io.openmessaging.benchmark.driver.pravega.schema.common;

public interface EventTimeStampAware {

    void setEventTimestamp(Long value);

    Long getEventTimestamp();

}
