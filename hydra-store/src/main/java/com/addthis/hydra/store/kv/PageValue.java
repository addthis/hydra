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

package com.addthis.hydra.store.kv;

import com.addthis.codec.Codec;

/**
 * wrapper around an individual (non-paged) value V that allows for selective
 * decoding of tree nodes from a page. Pages start off with a bunch of these.
 * <p/>
 * Does some crazy concurrency things. They might not be a good idea? Kind of cool though.
 */
final class PageValue<V extends IReadWeighable & Codec.BytesCodable> {

    private V value;
    private byte[] raw;
    private volatile V realValue;

    // TODO: encode type should just be part of the KeyCoder implementation
    private final KeyCoder.EncodeType encodeType;
    private final KeyCoder<?, V> keyCoder;

    PageValue(KeyCoder<?, V> keyCoder, byte[] raw, KeyCoder.EncodeType encodeType) {
        this.keyCoder = keyCoder;
        this.raw = raw;
        this.encodeType = encodeType;
    }

    @Override
    public String toString() {
        if (value != null) {
            return "PV:" + value;
        } else {
            if (raw != null) {
                return "PV:" + ("{raw:" + raw.length + "}");
            } else {
                return "PV:" + "null";
            }
        }
    }

    public V value() {
        if (value == null) {
            byte[] r = raw;
            if (realValue != null) {
                value = realValue;
            } else if (r != null) {
                realValue = keyCoder.valueDecode(r, encodeType);
                value = realValue;
                raw = null;
            }
        }
        return value;
    }
}
