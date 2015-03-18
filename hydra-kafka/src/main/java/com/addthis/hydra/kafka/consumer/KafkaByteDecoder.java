/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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