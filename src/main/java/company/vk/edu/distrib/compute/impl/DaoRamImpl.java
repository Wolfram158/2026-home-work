package company.vk.edu.distrib.compute.impl;

import company.vk.edu.distrib.compute.Dao;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class DaoRamImpl implements Dao<byte[]> {
    private final Map<String, byte[]> keyToValue;

    public DaoRamImpl() {
        keyToValue = new ConcurrentHashMap<>();
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException();
        } else if (!keyToValue.containsKey(key)) {
            throw new NoSuchElementException();
        }
        return keyToValue.get(key);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException {
        if (key == null || value == null) {
            throw new IllegalArgumentException();
        }
        keyToValue.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException();
        }
        keyToValue.remove(key);
    }

    @Override
    public void close() {
        // nothing to close
    }
}
