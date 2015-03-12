package com.addthis.hydra.kafka.consumer;

import java.io.IOException;

import com.addthis.bundle.channel.kvp.KVChannelSource;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.Bundles;
import com.addthis.bundle.core.kvp.KVBundle;
import com.addthis.bundle.core.kvp.KVBundleFormat;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.io.DataChannelCodec;

public interface KafkaByteDecoder {

    public enum KafkaByteDecoderType {BUNDLE, KV}

    public Bundle decodeBundle(byte[] bytes, ListBundleFormat format) throws IOException;

    public static class BundleDecoder implements KafkaByteDecoder {

        @Override
        public Bundle decodeBundle(byte[] bytes, ListBundleFormat format) throws IOException {
            return DataChannelCodec.decodeBundle(new ListBundle(format), bytes);
        }
    }

    public static class KVDecoder implements KafkaByteDecoder {

        @Override
        public Bundle decodeBundle(byte[] bytes, ListBundleFormat format) {
            // KVBundle/Formats are not thread-safe, so copy it to a ListBundle
            KVBundle kv =  KVChannelSource.fromText(bytes, new KVBundleFormat());
            return Bundles.addAll(kv, new ListBundle(format), true);
        }
    }
}