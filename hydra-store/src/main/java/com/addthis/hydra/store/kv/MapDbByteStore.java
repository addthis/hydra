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

import java.io.File;

import java.util.Iterator;
import java.util.Map;

import com.addthis.basis.util.LessFiles;

import com.google.common.primitives.UnsignedBytes;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapDbByteStore implements ByteStore {

    private static final Logger log = LoggerFactory.getLogger(MapDbByteStore.class);

    private final File dir;
    private final File dbFile;
    private final DB   db;
    private final BTreeMap<byte[], byte[]> btree;

    public MapDbByteStore(File dir, String dbname, boolean readonly) {
        this.dir = dir;
        LessFiles.initDirectory(dir);
        this.dbFile = new File(dir, "mapdb");

        DBMaker dbMaker = DBMaker.newFileDB(dbFile)
                .transactionDisable()
                .cacheDisable()
                .mmapFileEnableIfSupported();

        if (readonly) {
            dbMaker = dbMaker.readOnly();
        }

        this.db = dbMaker.make();
        this.btree = db.createTreeMap(dbname)
                .valuesOutsideNodesEnable()
                .comparator(UnsignedBytes.lexicographicalComparator())
                .makeOrGet();

        log.info("MapDB ByteStore started up for {}", dir);
    }

    public MapDbByteStore(File dir, String dbname) {
        this(dir, dbname, false);
    }

    @Override
    public boolean hasKey(byte[] key) {
        return btree.containsKey(key);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public byte[] firstKey() {
        return btree.firstKey();
    }

    @Override
    public byte[] lastKey() {
        return btree.lastKey();
    }

    @Override
    public byte[] delete(byte[] key) {
        return btree.remove(key);
    }

    @Override
    public void put(byte[] key, byte[] val) {
        btree.put(key, val);
    }

    @Override
    public byte[] get(byte[] key) {
        return btree.get(key);
    }

    @Override
    public byte[] higherKey(byte[] key) {
        return btree.higherKey(key);
    }

    @Override
    public byte[] lowerKey(byte[] key) {
        return btree.lowerKey(key);
    }

    @Override
    public byte[] floorKey(byte[] key) {
        return btree.floorKey(key);
    }

    @Override
    public Map.Entry<byte[], byte[]> floorEntry(byte[] key) {
        return btree.floorEntry(key);
    }

    // actually firstValue wtf
    @Override
    public byte[] firstEntry() {
        return btree.firstEntry().getValue();
    }

    @Override
    public Iterator<byte[]> keyIterator(byte[] start) {
        return btree.keySet().tailSet(start, false).iterator();
    }

    @Override
    public void close() {
        btree.close();
    }

    @Override
    public void close(boolean cleanLog) {
        if (cleanLog) {
            db.compact();
        }
        btree.close();
    }

    @Override
    public long count() {
        return btree.sizeLong();
    }
}
