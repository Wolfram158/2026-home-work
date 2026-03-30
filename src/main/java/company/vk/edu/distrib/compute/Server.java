package company.vk.edu.distrib.compute;

import company.vk.edu.distrib.compute.impl.KVServiceFactoryFileWithCacheImpl;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class Server {
    private Server() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        var log = LoggerFactory.getLogger("server");
        var port = 8080;
        KVService storage = new KVServiceFactoryFileWithCacheImpl().create(port);
        storage.start();
        log.info("Server started on port {}", port);
        Thread.currentThread().join();
        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
    }
}
