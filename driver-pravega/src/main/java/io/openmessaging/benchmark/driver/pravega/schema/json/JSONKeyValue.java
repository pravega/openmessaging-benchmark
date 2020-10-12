package io.openmessaging.benchmark.driver.pravega.schema.json;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@AllArgsConstructor
@NoArgsConstructor
public class JSONKeyValue {
    @Getter
    private String name;
    @Getter
    private String value;
}
