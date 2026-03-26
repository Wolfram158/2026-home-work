package company.vk.edu.distrib.compute.impl;

import company.vk.edu.distrib.compute.Dao;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class DaoFileWithCacheImpl implements Dao<byte[]> {
    private final Map<String, byte[]> keyToValueCache;
    private final Path dbPath;
    private final ReentrantLock lock;
    private static final String SEP = "<@>";

    public DaoFileWithCacheImpl() throws IOException {
        keyToValueCache = new ConcurrentHashMap<>();
        dbPath = Paths.get("./storage.db");
        if (!Files.exists(dbPath)) {
            Files.createFile(dbPath);
        }
        lock = new ReentrantLock();
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key == null) {
            throw new IllegalArgumentException();
        }
        final byte[] value = keyToValueCache.get(key);
        if (value != null) {
            return value;
        }
        String line;
        lock.lock();
        try (BufferedReader br = Files.newBufferedReader(dbPath)) {
            while ((line = br.readLine()) != null) {
                final String[] keyValue = line.split(SEP);
                if (key.equals(keyValue[0])) {
                    final byte[] realValue = keyValue[1].getBytes(StandardCharsets.UTF_8);
                    keyToValueCache.put(key, realValue);
                    return realValue;
                }
            }
            throw new NoSuchElementException();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (key == null || value == null) {
            throw new IllegalArgumentException();
        }
        try (
                BufferedReader br = Files.newBufferedReader(dbPath);
                BufferedWriter bw = Files.newBufferedWriter(dbPath)
        ) {
            final List<String> lines = new ArrayList<>(br
                    .lines()
                    .filter(line -> {
                        final String[] keyValue = line.split(SEP, 2);
                        return !keyValue[0].equals(key);
                    })
                    .toList());
            final String newValue = new String(value, StandardCharsets.UTF_8);
            final String newLine = String.format("%s%s%s", key, SEP, newValue);
            lines.add(newLine);
            lock.lock();
            keyToValueCache.put(key, value);
            bw.write(String.join(System.lineSeparator(), lines));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key == null) {
            throw new IllegalArgumentException();
        }
        try (
                BufferedReader br = Files.newBufferedReader(dbPath);
                BufferedWriter bw = Files.newBufferedWriter(dbPath)
        ) {
            final List<String> lines = br
                    .lines()
                    .filter(line -> {
                        final String[] keyValue = line.split(SEP, 2);
                        return !keyValue[0].equals(key);
                    })
                    .toList();
            lock.lock();
            keyToValueCache.remove(key);
            bw.write(String.join(System.lineSeparator(), lines));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        // nothing to close
    }
}
