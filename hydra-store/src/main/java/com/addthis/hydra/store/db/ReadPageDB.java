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

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import com.addthis.basis.util.Parameter;

import com.addthis.codec.Codec;
import com.addthis.codec.CodecBin2;
import com.addthis.hydra.store.kv.ByteStoreBDB;
import com.addthis.hydra.store.kv.ExternalPagedStore.ByteStore;
import com.addthis.hydra.store.kv.PagedKeyValueStore;
import com.addthis.hydra.store.kv.ReadExternalPagedStore;

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
public class ReadPageDB<V extends IReadWeighable & Codec.Codable> implements IPageDB<DBKey, V> {

    private static final Logger log = LoggerFactory.getLogger(ReadPageDB.class);
    static final String defaultDbName = Parameter.value("pagedb.dbname", "db.key");

    private final Codec codec = new CodecBin2();
    private final Class<? extends V> clazz;
    private final ReadExternalPagedStore<DBKey, V> eps;

    public ReadPageDB(File dir, Class<? extends V> clazz, int maxSize, int maxWeight) throws IOException {
        this(dir, clazz, maxSize, maxWeight, false);
    }

    public ReadPageDB(File dir, Class<? extends V> clazz, int maxSize,
            int maxWeight, boolean metrics) throws IOException {
        this.clazz = clazz;
        ByteStore store = new ByteStoreBDB(dir, defaultDbName, true);
        this.eps = new ReadExternalPagedStore<>(new ReadDBKeyCoder<>(codec, clazz), store, maxSize, maxWeight, metrics);
    }

    public String toString() {
        return "PageDB:" + clazz + "," + eps;
    }

    @Override
    public V get(DBKey key) {
        return eps.getValue(key);
    }

    public TreeMap<DBKey, V> toTreeMap() {
        try {
            IPageDB.Range<DBKey, V> range = range(this.eps.getFirstKey(), new DBKey(Integer.MAX_VALUE, ""));
            Iterator<Map.Entry<DBKey, V>> iterator = range.iterator();
            TreeMap<DBKey, V> map = new TreeMap<>();
            while (iterator.hasNext()) {
                Map.Entry<DBKey, V> entry = iterator.next();
                map.put(entry.getKey(), entry.getValue());
            }
            return map;
        } catch (Exception e) {
            log.warn("failed to dump PageDB to Map, returning empty (expected for uninitialized db)", e);
            return new TreeMap<>();
        }
    }

    //Uses the DR object from below.
    @Override
    public IPageDB.Range<DBKey, V> range(DBKey from, DBKey to) {
        return new DR(from, to, 1);
    }

    public IPageDB.Range<DBKey, V> range(DBKey from, DBKey to, int sampleRate) {
        return new DR(from, to, sampleRate);
    }

    /**
     * Close the source.
     *
     * @param cleanLog unused in the ReadPageDB implementation.
     * @param operation unused in the ReadPageDB implementation.
     * @return status code. A status code of 0 indicates success.
     */
    @Override
    public int close(boolean cleanLog, CloseOperation operation) {
        close();
        return 0;
    }

    @Override
    public void close() {
        eps.close();
    }

    /**
     * Wraps eps range and adds RIGHT-side bounding (on top of the LEFT-side bounding
     * provided by the eps object). Not thread safe.
     */
    private class DR implements IPageDB.Range<DBKey, V>, Iterator<Map.Entry<DBKey, V>> {

        private final Iterator<Map.Entry<DBKey, V>> iter;
        private final DBKey to;

        private Map.Entry<DBKey, V> next;

        private DR(DBKey start, DBKey to, int sampleRate) {
            if (log.isDebugEnabled()) log.debug("DR(" + start + "-" + to + ")");
            this.iter = eps.range(start, true, sampleRate);
            this.to = to;
        }

        @Override
        public boolean hasNext() {
            if (next == null && iter.hasNext()) {
                next = iter.next();
                if (to != null && next.getKey().compareTo(to) >= 0) {
                    if (log.isDebugEnabled()) log.debug("stopping range b/c " + next.getKey() + " >= " + to);
                    close();
                    next = null;
                }
            }
            return next != null;
        }

        @Override
        public void close() {
        }

        @Override
        public Map.Entry<DBKey, V> next() {
            if (hasNext()) {
                Map.Entry<DBKey, V> ret = next;
                next = null;
                return ret;
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<Map.Entry<DBKey, V>> iterator() {
            return this;
        }
    }

    //Unsupported interface compatability stuff
    public void setCacheSize(final int cachesize) {
        throw new UnsupportedOperationException();
    }

    public void setPageSize(int pagesize) {
        throw new UnsupportedOperationException();
    }

    public void setCacheMem(long maxmem) {
        throw new UnsupportedOperationException();
    }

    public void setPageMem(int maxmem) {
        throw new UnsupportedOperationException();
    }

    public void setMemSampleInterval(int sample) {
        throw new UnsupportedOperationException();
    }

    public V put(DBKey key, V value) {
        throw new UnsupportedOperationException();
    }

    public V remove(DBKey key) {
        throw new UnsupportedOperationException();
    }

    public void remove(DBKey start, DBKey end, boolean inclusive) {
        throw new UnsupportedOperationException();
    }

    public ReadExternalPagedStore<DBKey, V> getReadEps() {
        return eps;
    }

    @Override
    public PagedKeyValueStore<DBKey, V> getEps() { throw new UnsupportedOperationException(); }

}
