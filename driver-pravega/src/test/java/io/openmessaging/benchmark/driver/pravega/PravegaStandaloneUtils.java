package io.openmessaging.benchmark.driver.pravega;

import io.pravega.local.LocalPravegaEmulator;
import lombok.SneakyThrows;
import lombok.Synchronized;

import java.util.concurrent.atomic.AtomicReference;

public class PravegaStandaloneUtils {

    private static final AtomicReference<PravegaStandaloneUtils> SINGLETON = new AtomicReference<>();
    private final LocalPravegaEmulator localPravega;

    @SneakyThrows
    private PravegaStandaloneUtils() {
        LocalPravegaEmulator.LocalPravegaEmulatorBuilder emulatorBuilder = LocalPravegaEmulator
                .builder()
                .controllerPort(9090)
                .segmentStorePort(1234)
                .zkPort(2180)
                .restServerPort(9091)
                .enableRestServer(false)
                .enableAuth(false)
                .enableTls(false);

        localPravega = emulatorBuilder.build();
        localPravega.getInProcPravegaCluster().start();
    }

    @Synchronized
    public static PravegaStandaloneUtils startPravega() {
        if (SINGLETON.get() == null) {
            SINGLETON.set(new PravegaStandaloneUtils());
        }
        return SINGLETON.get();
    }

    public String getControllerURI() {
        return localPravega.getInProcPravegaCluster().getControllerURI();
    }
}
