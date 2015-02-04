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
package com.addthis.hydra.store.db;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.addthis.codec.Codec;
import com.addthis.codec.binary.CodecBin2;
import com.addthis.codec.codables.BytesCodable;
import com.addthis.hydra.store.kv.PageEncodeType;
import com.addthis.hydra.store.kv.KeyCoder;
import com.addthis.hydra.store.kv.TreeEncodeType;
import com.addthis.hydra.store.util.Raw;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;

/**
 */
class DBKeyCoder<V extends BytesCodable> implements KeyCoder<DBKey, V> {

    protected final Codec codec;
    protected static final CodecBin2 codecBin2 = CodecBin2.INSTANCE;
    protected final Class<? extends V> clazz;

    private static final byte[] zero = new byte[0];

    public DBKeyCoder(Class<? extends V> clazz) {
        this(codecBin2, clazz);
    }

    public DBKeyCoder(Codec codec, Class<? extends V> clazz) {
        this.codec = codec;
        this.clazz = clazz;
    }

    @Override
    public DBKey negInfinity() {
        return new DBKey(0, (Raw) null);
    }

    /**
     * Unoptimized key encoding. Uses only the state of the key
     * to generate byte array. The sorted order of two keys should
     * be equal to the natural ordering of their corresponding byte arrays.
     *
     * @param key
     * @return
     */
    @Override
    public byte[] keyEncode(DBKey key, @Nonnull TreeEncodeType encodeType) {
        if (key == null) {
            return zero;
        } else {
            return key.toBytes(encodeType);
        }
    }

    /**
     * Optimized key encoding. Can use the base key to generate
     * a smaller byte array.
     *
     * @param key
     * @param baseKey
     * @param encodeType
     * @return
     */
    @Override
    public byte[] keyEncode(@Nullable DBKey key, @Nonnull DBKey baseKey, @Nonnull PageEncodeType encodeType) {
        if (key == null) {
            return zero;
        }
        switch (encodeType) {
            case LEGACY:
            case SPARSE:
                return key.toBytes(encodeType.getTreeType());
            case LONGIDS:
                return key.deltaEncode(baseKey);
            default:
                throw new RuntimeException("Unknown encoding type: " + encodeType);
        }
    }

    @Override
    public byte[] valueEncode(V value, @Nonnull PageEncodeType encodeType) {
        try {
            switch (encodeType) {
                case LEGACY:
                    return codec.encode(value);
                case SPARSE:
                case LONGIDS:
                    if (value == null) {
                        return zero;
                    } else {
                        return value.bytesEncode(encodeType.ordinal());
                    }
                default:
                    throw new RuntimeException("Unknown encoding type: " + encodeType);
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Unoptimized key encoding. Uses only the state of the
     * byte array to generate the key. The sorted order of
     * two keys should be equal to the natural ordering of
     * their corresponding byte arrays.
     *
     * @param key
     * @return
     */
    @Override
    public DBKey keyDecode(byte[] key, @Nonnull TreeEncodeType encodeType) {
        if (key == null || key.length == 0) {
            return null;
        } else {
            return DBKey.fromBytes(key, encodeType);
        }
    }

    /**
     * Optimized key decoding. Can use the byte array
     * and the base key to generate the key.
     *
     * @param key
     * @param baseKey
     * @param encodeType
     * @return
     */
    @Override
    public DBKey keyDecode(@Nullable byte[] key, @Nonnull DBKey baseKey, @Nonnull PageEncodeType encodeType) {
        if (key == null || key.length == 0) {
            return null;
        } else {
            switch (encodeType) {
                case LEGACY:
                case SPARSE:
                    return DBKey.fromBytes(key, encodeType.getTreeType());
                case LONGIDS:
                    return DBKey.deltaDecode(key, baseKey);
                default:
                    throw new RuntimeException("Unknown encoding type: " + encodeType);
            }
        }
    }

    @Override
    public V valueDecode(byte[] value, @Nonnull PageEncodeType encodeType) {

        try {
            switch (encodeType) {
                case LEGACY:
                    return codec.decode(clazz.newInstance(), value);
                case SPARSE:
                case LONGIDS:
                    if (value.length > 0) {
                        V v = clazz.newInstance();
                        v.bytesDecode(value, encodeType.ordinal());
                        return v;
                    } else {
                        return null;
                    }
                default:
                    throw new RuntimeException("Unknown encoding type: " + encodeType);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean nullRawValueInternal(byte[] value) {
        return codec.storesNull(value);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("codec", codec)
                .add("clazz", clazz)
                .toString();
    }
}
