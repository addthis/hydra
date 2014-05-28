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

import java.io.File;
import java.io.IOException;

import com.addthis.basis.util.Parameter;

import com.addthis.codec.Codec;
import com.addthis.codec.CodecBin2;
import com.addthis.hydra.store.kv.ByteStore;
import com.addthis.hydra.store.kv.ByteStoreBDB;
import com.addthis.hydra.store.kv.IReadWeighable;
import com.addthis.hydra.store.kv.MapDbByteStore;
import com.addthis.hydra.store.kv.ReadPageCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * wrapper around ReadExternalPagedStore
 * <p/>
 * It mostly provides two things:
 * <p/>
 * 1. encode / decode implementations
 * 2. a data range object that notably has RIGHT-side bounded iteration (left as well
 * but that is stolen from an iterator it wraps. There are like five nested iterators).
 */
public class ReadPageDB {

    private static final Logger log = LoggerFactory.getLogger(ReadPageDB.class);

    private static final String DEFAULT_DB_NAME = Parameter.value("pagedb.dbname", "db.key");
    private static final Codec codec = new CodecBin2();

    public static <V extends IReadWeighable & Codec.BytesCodable> ReadPageCache<DBKey, V> newPageCache(
            File dir, Class<? extends V> clazz, int maxSize, int maxWeight) throws IOException {
        return newPageCache(dir, new ReadDBKeyCoder<>(codec, clazz), maxSize, maxWeight);
    }

    public static <V extends IReadWeighable & Codec.BytesCodable> ReadPageCache<DBKey, V> newPageCache(
            File dir, DBKeyCoder<V> keyCoder, int maxSize, int maxWeight) throws IOException {
        String dbType = PageDB.getByteStoreNameForFile(dir);
        ByteStore store;
        switch (dbType) {
            case PageDB.PAGED_MAP_DB:
                store = new MapDbByteStore(dir, DEFAULT_DB_NAME, true);
                break;
            case PageDB.PAGED_BERK_DB:
                // fall through -- the previous dbType was always something like 'pagedb' so this is expected
            default:
                store = new ByteStoreBDB(dir, DEFAULT_DB_NAME, true);
                break;
        }
        return new ReadPageCache<>(keyCoder, store, maxSize, maxWeight);
    }
}
