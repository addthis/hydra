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
package com.addthis.hydra.data.query;


import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.PageDB;

import org.apache.commons.io.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiskBackedMap<T extends DiskBackedMap.DiskObject> implements Map<String, T>, Closeable {

    private static final Logger log = LoggerFactory.getLogger(DiskBackedMap.class);
    //private final PageDB<CodableDiskObject> db;
    // By all rights this should be the above, but generics and inner
    // classes do not seem to play nice
    private final PageDB db;
    private final String diskStoragePath;
    private final File diskStoragePathFile;
    private final DiskObjectFactory factory;
    private long cacheSize;

    public DiskBackedMap(String diskStoragePath, DiskObjectFactory factory, long cacheSize) {
        this.diskStoragePath = diskStoragePath;
        this.factory = factory;
        this.cacheSize = cacheSize;

        // The smallest cache size possible with je is 96k. If the
        // user requests less cache, then use 1M
        if (this.cacheSize < 1024 * 1024) {
            this.cacheSize = 1024 * 1024L;
        }

        try {
            diskStoragePathFile = new File(diskStoragePath).getCanonicalFile();
        } catch (IOException e) {
            log.warn("Error while retrieving the canonical file for: {}", diskStoragePath);
            throw new RuntimeException(e);
        }
        if (diskStoragePathFile.exists()) {
            try {
                FileUtils.deleteDirectory(diskStoragePathFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (!diskStoragePathFile.mkdirs()) {
            log.warn("Error while creating the directory: {}", diskStoragePath);
            throw new RuntimeException("Error while creating the directory: " + diskStoragePath);
        }

        try {
            db = new PageDB.Builder<>(diskStoragePathFile, CodableDiskObject.class, 1000, 1000)
                    .dbname("DiskBackedMap.db").build();
            db.setCacheMem(cacheSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean containsKey(Object o) {
        return db.get(new DBKey(0, o.toString())) != null;
    }

    @Override
    public boolean containsValue(Object o) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public T get(Object o) {
        Object val = db.get(new DBKey(0, o.toString()));
        if (val != null) {
            return (T) ((CodableDiskObject) val).getDiskObject();
        } else {
            return null;
        }
    }

    @Override
    public T put(String s, T t) {
        Object old = db.put(new DBKey(0, s.toString()), new CodableDiskObject(t));
        if (old != null) {
            return (T) ((CodableDiskObject) old).getDiskObject();
        } else {
            return null;
        }
    }

    @Override
    public T remove(Object o) {
        return (T) ((CodableDiskObject) db.remove(new DBKey(0, o.toString()))).getDiskObject();
    }

    @Override
    public void putAll(Map<? extends String, ? extends T> map) {
        for (Entry<? extends String, ? extends T> e : map.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Set<String> keySet() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Set<Entry<String, T>> entrySet() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Collection<T> values() {
        List<T> list = new ArrayList<>();
        for (Object obj : db.toTreeMap().values()) {
            list.add((T) ((CodableDiskObject) obj).getDiskObject());
        }
        return list;
    }

    @Override
    public void close() throws IOException {
        // Close the database
        db.close();
        FileUtils.deleteDirectory(diskStoragePathFile);
    }


    public interface DiskObject {

        public abstract byte[] toBytes();
    }

    public interface DiskObjectFactory {

        public abstract DiskObject fromBytes(byte[] bytes);
    }

    // funky non-static class to maintain compatibility with
    // DiskObject interface
    public class CodableDiskObject implements Codec.SuperCodable, Codec.BytesCodable {

        private DiskObject d;

        @Codec.Set(codable = true)
        private byte[]  bytes;

        public CodableDiskObject() {
        }

        public CodableDiskObject(DiskObject d) {
            this.d = d;
        }

        public DiskObject getDiskObject() {
            return d;
        }

        @Override
        public void postDecode() {
            d = factory.fromBytes(bytes);
        }

        @Override
        public void preEncode() {
            bytes = d.toBytes();
        }

        @Override
        public byte[] bytesEncode(long version) {
            preEncode();
            return bytes;
        }

        @Override
        public void bytesDecode(byte[] b, long version) {
            bytes = b;
            postDecode();
        }
    }
}
