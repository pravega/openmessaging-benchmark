package io.openmessaging.benchmark.driver.pravega.schema.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.openmessaging.benchmark.driver.pravega.schema.common.EventTimeStampAware;
import lombok.*;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class JSONUser implements EventTimeStampAware {
    private String userId;
    private String name;
    private String biography;
    private List<JSONAddressEntry> address;
    private int age;
    private Long eventTimestamp;
    private List<JSONKeyValue> keyValues;
    private List<JSONKeyValue> keyValues2;
    private List<JSONKeyValue> keyValues3;
}