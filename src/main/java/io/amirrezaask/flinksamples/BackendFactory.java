package io.amirrezaask.flinksamples;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;

import java.io.IOException;

public class BackendFactory {
    public static RocksDBStateBackend getRocksDBBackend(String path) throws IOException {
        return new RocksDBStateBackend(path);
    }
}
