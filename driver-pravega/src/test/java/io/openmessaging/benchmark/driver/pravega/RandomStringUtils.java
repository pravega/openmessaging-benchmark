package io.openmessaging.benchmark.driver.pravega;

import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.apache.commons.text.RandomStringGenerator;
import org.junit.Assert;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class RandomStringUtils {

    public static String generateRandomString(int size) throws NoSuchAlgorithmException {
        SecureRandom number = SecureRandom.getInstanceStrong();
        byte[] array = number.generateSeed(size);
        return new String(array, StandardCharsets.UTF_8);
    }

    public static String generateRandomString2(int size)  {
        UniformRandomProvider rng = RandomSource.create(RandomSource.MT);
        RandomStringGenerator generator = new RandomStringGenerator.Builder().withinRange('A', 'z').usingRandom(rng::nextInt).build();
        String s = generator.generate(size);
        int payload = s.getBytes(StandardCharsets.UTF_8).length;
        Assert.assertEquals(size, payload);
        return s;
    }

    public static String generateRandomString3(int size)  {
        UniformRandomProvider rng = RandomSource.create(RandomSource.MT);
        RandomStringGenerator generator = new RandomStringGenerator.Builder().withinRange('A', 'Z').usingRandom(rng::nextInt).build();
        String s = generator.generate(size);
        int payload = s.getBytes(StandardCharsets.UTF_8).length;
        Assert.assertEquals(size, payload);
        return s;
    }
}
