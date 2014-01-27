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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.kv.ByteStoreBDB;
import com.addthis.hydra.store.kv.ConcurrentByteStoreBDB;
import com.addthis.hydra.store.kv.ExternalPagedStore;
import com.addthis.hydra.store.kv.ExternalPagedStore.ByteStore;
import com.addthis.hydra.store.kv.PagedKeyValueStore;
import com.addthis.hydra.store.skiplist.SkipListCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * wrapper around ExternalPagedStore
 */
public class PageDB<V extends Codec.Codable> implements IPageDB<DBKey, V> {

    private static final Logger log = LoggerFactory.getLogger(PageDB.class);

    static final String defaultDbName = Parameter.value("pagedb.dbname", "db.key");
    static final int defaultKeyValueStoreType = Parameter.intValue("pagedb.kvstore.type", 0);

    private final boolean readonly;
    private final PagedKeyValueStore<DBKey, V> eps;
    private final DBKeyCoder<V> keyCoder;
    private final HashSet<DR> openRanges = new HashSet<>();

    public static class Builder<V extends Codec.Codable> {

        // Required parameters
        protected final File dir;
        protected final Class<? extends V> clazz;
        protected final int maxPageSize;
        protected final int maxPages;

        // Optional parameters - initialized to default values;
        protected int kvStoreType = defaultKeyValueStoreType;
        protected String dbname = defaultDbName;
        protected boolean readonly = false;

        public Builder(File dir, Class<? extends V> clazz, int maxPageSize, int maxPages) {
            this.dir = dir;
            this.clazz = clazz;
            this.maxPageSize = maxPageSize;
            this.maxPages = maxPages;
        }

        public Builder kvStoreType(int value) {
            this.kvStoreType = value;
            return this;
        }

        public Builder readonly(boolean value) {
            this.readonly = value;
            return this;
        }

        public Builder dbname(String value) {
            this.dbname = value;
            return this;
        }

        public PageDB<V> build() throws Exception {
            return new PageDB<>(dir, clazz, dbname, maxPageSize, maxPages, kvStoreType, readonly);
        }

    }

    public PageDB(File dir, Class<? extends V> clazz, int maxPageSize, int maxPages) throws IOException {
        this(dir, clazz, defaultDbName, maxPageSize, maxPages, defaultKeyValueStoreType, false);
    }

    public PageDB(File dir, Class<? extends V> clazz, int maxPageSize, int maxPages, boolean readonly) throws IOException {
        this(dir, clazz, defaultDbName, maxPageSize, maxPages, defaultKeyValueStoreType, readonly);
    }

    public PageDB(File dir, Class<? extends V> clazz, String dbname, int maxPageSize,
            int maxPages, boolean readonly) throws IOException {
        this(dir, clazz, dbname, maxPageSize, maxPages, defaultKeyValueStoreType, readonly);
    }

    public PageDB(File dir, Class<? extends V> clazz, String dbname, int maxPageSize,
            int maxPages, int keyValueStoreType, boolean readonly) throws IOException {
        ByteStore store;
        this.readonly = readonly;
        this.keyCoder = new DBKeyCoder<>(clazz);
        switch (keyValueStoreType) {
            case 0:
                store = new ByteStoreBDB(dir, dbname, readonly);
                this.eps = new ExternalPagedStore<>(keyCoder, store, maxPageSize, maxPages);
                break;
            case 1:
                store = new ConcurrentByteStoreBDB(dir, dbname, readonly);
                this.eps = new SkipListCache<>(keyCoder, store, maxPageSize, maxPages);
                break;
            default:
                throw new IllegalStateException("Illegal value " + keyValueStoreType +
                                                " for configuration parameter \"pagedb.kvstore.type\"");
        }
        if (!readonly) {
            Files.write(new File(dir, "db.type"), Bytes.toBytes(getClass().getName()), false);
        }
    }

    @Override
    public String toString() {
        return "PageDB:" + keyCoder + "," + eps;
    }

    @Override
    public V get(DBKey key) {
        return eps.getValue(key);
    }

    @Override
    public V put(DBKey key, V value) {
        if (readonly) {
            throw new RuntimeException("cannot modify. readonly.");
        }
        return eps.getPutValue(key, value);
    }

    @Override
    public V remove(DBKey key) {
        if (readonly) {
            throw new RuntimeException("cannot modify. readonly.");
        }
        return eps.getRemoveValue(key);
    }

    @Override
    public void remove(DBKey from, DBKey to, boolean inclusive) {
        if (readonly) {
            throw new RuntimeException("cannot modify. readonly.");
        }
        eps.removeValues(from, to, inclusive);
    }

    public TreeMap<DBKey, V> toTreeMap() {
        try {
            Range<DBKey, V> range = this.range(this.eps.getFirstKey(), new DBKey(Integer.MAX_VALUE, ""));
            Iterator<Entry<DBKey, V>> iterator = range.iterator();
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

    @Override
    public IPageDB.Range<DBKey, V> range(DBKey from, DBKey to) {
        return new DR(from, to);
    }

    @Override
    public void close() {
        close(false, CloseOperation.NONE);
    }

    @Override
    public PagedKeyValueStore<DBKey, V> getEps() {
        return eps;
    }

    /**
     * Close the source.
     *
     * @param cleanLog if true then wait for the BerkeleyDB clean thread to finish.
     * @param operation optionally test or repair the berkeleyDB.
     * @return status code. A status code of 0 indicates success.
     */
    @Override
    public int close(boolean cleanLog, CloseOperation operation) {
        try {
            synchronized (openRanges) {
                if (openRanges.size() > 0) {
                    log.warn("closing " + openRanges.size() + " ranges on close");
                }
                for (Object dr : openRanges.toArray(new Object[openRanges.size()])) {
                    ((DR) dr).close();
                }
            }
        } finally {
            int status = eps.close(cleanLog, operation);
            return status;
        }
    }

    @Override
    public void setCacheSize(int cachesize) {
        eps.setMaxPages(cachesize);
    }

    @Override
    public void setPageSize(int pagesize) {
        eps.setMaxPageSize(pagesize);
    }

    @Override
    public void setCacheMem(long maxmem) {
        eps.setMaxTotalMem(maxmem);
    }

    @Override
    public void setPageMem(int maxmem) {
        eps.setMaxPageMem(maxmem);
    }

    @Override
    public void setMemSampleInterval(int sample) {
        eps.setMemEstimateInterval(sample);
    }

    private class DR implements IPageDB.Range<DBKey, V>, Iterator<Entry<DBKey, V>> {

        private final Iterator<Entry<DBKey, V>> iter;
        private final DBKey to;

        private Entry<DBKey, V> next;

        private DR(DBKey start, DBKey to) {
            log.debug("DR(" + start + "-" + to + ")");
            this.iter = eps.range(start, true);
            this.to = to;
            synchronized (openRanges) {
                openRanges.add(this);
            }
        }

        @Override
        public void close() {
            if (iter instanceof ClosableIterator) {
                ((ClosableIterator) iter).close();
            }
            synchronized (openRanges) {
                openRanges.remove(this);
            }
        }

        @Override
        public boolean hasNext() {
            if (next == null && iter.hasNext()) {
                next = new Entry<DBKey, V>() {
                    private final Entry<DBKey, V> next = iter.next();

                    {
                        log.debug("DR next=" + next + (next != null ? " " + next.getKey() : ""));
                    }

                    @Override
                    public DBKey getKey() {
                        return next.getKey();
                    }

                    @Override
                    public V getValue() {
                        return next.getValue();
                    }

                    @Override
                    public V setValue(V value) {
                        throw new UnsupportedOperationException();
                    }
                };
                if (to != null && next.getKey().compareTo(to) >= 0) {
                    log.debug("stopping range b/c " + next.getKey() + " >= " + to);
                    close();
                    next = null;
                }
            }
            return next != null;
        }

        @Override
        public Entry<DBKey, V> next() {
            if (hasNext()) {
                Entry<DBKey, V> ret = next;
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
        public Iterator<Entry<DBKey, V>> iterator() {
            return this;
        }
    }
}
