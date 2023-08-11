package com.glodon.sink;

import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;

public class RocketmqSink implements StatefulSink {
    @Override
    public StatefulSinkWriter createWriter(InitContext initContext) throws IOException {
        return null;
    }

    @Override
    public StatefulSinkWriter restoreWriter(InitContext initContext, Collection collection) throws IOException {
        return null;
    }

    @Override
    public SimpleVersionedSerializer getWriterStateSerializer() {
        return null;
    }
}
