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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;

import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Varint;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.db.PageKey;

import com.google.common.cache.CacheLoader;

import com.ning.compress.lzf.LZFInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

class ReadPageCacheLoader<K extends Comparable<K> & PageKey, V extends IReadWeighable & Codec.BytesCodable>
        extends CacheLoader<K, ReadTreePage<K, V>> {

    private static final Logger log = LoggerFactory.getLogger(ReadPageCacheLoader.class);

    static final int FLAGS_IS_SPARSE = 1 << 5;

    private final ByteStore pages;
    private final KeyCoder<K, V> keyCoder;

    public ReadPageCacheLoader(ByteStore pages, KeyCoder<K, V> keyCoder) {
        this.pages = pages;
        this.keyCoder = keyCoder;
    }

    @Override
    public ReadTreePage<K, V> load(K key) throws Exception {
        byte[] page = pages.get(keyCoder.keyEncode(key));
        if (page != null) {
            return pageDecode(page);
        } else {
            throw new ExecutionException("Source did not have page", new NullPointerException());
        }
    }

    //decode pages. Called on the bytes returned by store.get()
    // TODO: this could be much more efficient; why were all the changes reverted?
    private ReadTreePage<K, V> pageDecode(byte[] page) {
        try {
            InputStream in = new ByteArrayInputStream(page);
            int flags = in.read() & 0xff;
            int gztype = flags & 0x0f;
            boolean isSparse = (flags & FLAGS_IS_SPARSE) != 0;
            switch (gztype) {
                case 1:
                    in = new InflaterInputStream(in);
                    break;
                case 2:
                    in = new GZIPInputStream(in);
                    break;
                case 3:
                    in = new LZFInputStream(in);
                    break;
                case 4:
                    in = new SnappyInputStream(in);
                    break;
            }
            ReadTreePage<K, V> decode;
            if (isSparse) {
                DataInputStream dis = new DataInputStream(in);
                int entries = Varint.readUnsignedVarInt(dis);
                int count = entries;

                K firstKey = keyCoder.keyDecode(Bytes.readBytes(in, Varint.readUnsignedVarInt(dis)));
                int nextFirstKeyLength = Varint.readUnsignedVarInt(dis);
                K nextFirstKey = null;
                if (nextFirstKeyLength > 0) {
                    nextFirstKey = keyCoder.keyDecode(Bytes.readBytes(in, nextFirstKeyLength));
                }
                decode = new ReadTreePage<K, V>(firstKey).setNextFirstKey(nextFirstKey);
                while (count-- > 0) {
                    byte[] kb = Bytes.readBytes(in, Varint.readUnsignedVarInt(dis));
                    byte[] vb = Bytes.readBytes(in, Varint.readUnsignedVarInt(dis));
                    K key = keyCoder.keyDecode(kb);
                    decode.map.put(key, new PageValue<>(keyCoder, vb, KeyCoder.EncodeType.SPARSE));
                }

            } else {
                int entries = (int) Bytes.readLength(in);
                K firstKey = keyCoder.keyDecode(Bytes.readBytes(in));
                K nextFirstKey = keyCoder.keyDecode(Bytes.readBytes(in));
                decode = new ReadTreePage<K, V>(firstKey).setNextFirstKey(nextFirstKey);
                while (entries-- > 0) {
                    byte[] kb = Bytes.readBytes(in);
                    byte[] vb = Bytes.readBytes(in);
                    K key = keyCoder.keyDecode(kb);
                    decode.map.put(key, new PageValue<>(keyCoder, vb, KeyCoder.EncodeType.LEGACY));
                }
                //ignoring memory data
                in.close();
                log.debug("decoded {}", decode);
            }

            decode.setWeight(page.length);
            return decode;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
