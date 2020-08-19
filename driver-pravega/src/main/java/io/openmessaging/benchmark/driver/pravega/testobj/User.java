package io.openmessaging.benchmark.driver.pravega.testobj;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {
    @Getter
    private Long eventTimestamp;
    @Getter
    private String name;
    @Getter
    private Address address;
    @Getter
    private int age;

}
