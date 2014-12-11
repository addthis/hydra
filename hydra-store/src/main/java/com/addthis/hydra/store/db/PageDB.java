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
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;

import com.addthis.codec.codables.BytesCodable;
import com.addthis.hydra.store.kv.ByteStore;
import com.addthis.hydra.store.kv.ConcurrentByteStoreBDB;
import com.addthis.hydra.store.kv.MapDbByteStore;
import com.addthis.hydra.store.kv.PagedKeyValueStore;
import com.addthis.hydra.store.skiplist.Page;
import com.addthis.hydra.store.skiplist.PageFactory;
import com.addthis.hydra.store.skiplist.SkipListCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * wrapper around ExternalPagedStore
 */
public class PageDB<V extends BytesCodable> implements IPageDB<DBKey, V> {

    private static final Logger log = LoggerFactory.getLogger(PageDB.class);

    static final String PAGED_MAP_DB = "paged.mapdb";
    static final String PAGED_BERK_DB = "paged.bdb";

    static final String defaultDbName = Parameter.value("pagedb.dbname", "db.key");
    static final String DEFAULT_BYTESTORE = Parameter.value("pagedb.bytestore", PAGED_BERK_DB);

    private final PagedKeyValueStore<DBKey, V> eps;
    private final DBKeyCoder<V> keyCoder;
    private final HashSet<DR> openRanges = new HashSet<>();

    public static class Builder<V extends BytesCodable> {

        // Required parameters
        protected final File dir;
        protected final Class<? extends V> clazz;
        protected final int maxPageSize;
        protected final int maxPages;

        // Optional parameters - initialized to default values;
        protected String dbname = defaultDbName;
        protected PageFactory pageFactory = Page.DefaultPageFactory.singleton;

        public Builder(File dir, Class<? extends V> clazz, int maxPageSize, int maxPages) {
            this.dir = dir;
            this.clazz = clazz;
            this.maxPageSize = maxPageSize;
            this.maxPages = maxPages;
        }

        public Builder dbname(String value) {
            this.dbname = value;
            return this;
        }

        public Builder pageFactory(PageFactory factory) {
            this.pageFactory = factory;
            return this;
        }

        public PageDB<V> build() throws IOException {
            return new PageDB<>(dir, clazz, dbname, maxPageSize, maxPages, pageFactory);
        }
    }

    protected PageDB(PagedKeyValueStore<DBKey, V> eps, DBKeyCoder<V> keyCoder) {
        this.eps = eps;
        this.keyCoder = keyCoder;
    }

    public PageDB(File dir, Class<? extends V> clazz, int maxPageSize, int maxPages) throws IOException {
        this(dir, clazz, defaultDbName, maxPageSize, maxPages, Page.DefaultPageFactory.singleton);
    }

    public PageDB(File dir, Class<? extends V> clazz, String dbname, int maxPageSize,
                  int maxPages, PageFactory factory) throws IOException {
        this.keyCoder = new DBKeyCoder<>(clazz);
        String dbType = getByteStoreNameForFile(dir);
        ByteStore store;
        switch (dbType) {
            case PAGED_MAP_DB:
                store = new MapDbByteStore(dir, dbname);
                break;
            case PAGED_BERK_DB:
                // fall through -- the previous dbType was always something like 'pagedb' so this is expected
            default:
                store = new ConcurrentByteStoreBDB(dir, dbname);
                break;
        }
        this.eps =  new SkipListCache.Builder<>(keyCoder, store, maxPageSize, maxPages).
        pageFactory(factory).build();
        Files.write(new File(dir, "db.type"), Bytes.toBytes(dbType), false);
    }

    public static String getByteStoreNameForFile(File dir) throws IOException {
        File typeFile = new File(dir, "db.type");
        if (typeFile.exists()) {
            return new String(Files.read(typeFile));
        } else {
            return DEFAULT_BYTESTORE;
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
        return eps.getPutValue(key, value);
    }

    @Override
    public V remove(DBKey key) {
        return eps.getRemoveValue(key);
    }

    @Override
    public void remove(DBKey from, DBKey to, boolean inclusive) {
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
            log.error("failed to dump PageDB to Map, returning empty (expected for uninitialized db)", e);
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
        int status;
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
            status = eps.close(cleanLog, operation);
        }
        return status;
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

        private final AtomicBoolean shutdownGuard = new AtomicBoolean();

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
            if (!shutdownGuard.getAndSet(true)) {
                if (iter instanceof ClosableIterator) {
                    ((ClosableIterator) iter).close();
                }
                synchronized (openRanges) {
                    openRanges.remove(this);
                }
            }
        }

        /**
         * Returns true if the iteration has more elements.
         * If the iteration has no more elements then
         * {@link DR#close} will be invoked.
         *
         * @return true if the iteration has more elements.
         */
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
            if (next == null) {
                close();
                return false;
            } else {
                return true;
            }
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
