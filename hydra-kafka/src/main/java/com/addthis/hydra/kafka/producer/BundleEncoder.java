package com.addthis.hydra.kafka.producer;

import java.io.IOException;

import java.util.Map;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.io.DataChannelCodec;

import org.apache.kafka.common.serialization.Serializer;

public class BundleEncoder implements Serializer<Bundle> {

    @Override
    public void configure(Map<String, ?> stringMap, boolean b) {
        // not needed
    }

    @Override
    public byte[] serialize(String topic, Bundle bundle) {
        if(bundle == null) {
            return null;
        }
        try {
            return DataChannelCodec.encodeBundle(bundle);
        } catch (IOException e) {
            //this exception is never actually thrown
            return null;
        }
    }

    @Override
    public void close() {
        // not needed
    }
}
