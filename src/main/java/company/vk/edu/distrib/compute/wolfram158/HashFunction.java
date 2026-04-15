package company.vk.edu.distrib.compute.wolfram158;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashFunction {
    public BigInteger getHash(String key) {
        final MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        final byte[] hashBytes = md.digest(key.getBytes(StandardCharsets.UTF_8));
        final StringBuilder sb = new StringBuilder();
        for (byte b : hashBytes) {
            sb.append(String.format("%02x", b));
        }
        return new BigInteger(sb.toString(), 16);
    }
}
