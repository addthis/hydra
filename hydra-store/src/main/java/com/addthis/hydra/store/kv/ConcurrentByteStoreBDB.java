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

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.Files;

import com.addthis.hydra.store.db.SettingsJE;
import com.addthis.hydra.store.util.JEUtil;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * simple byte store that complies with ByteStore for use with
 * SkipListCache
 */
public class ConcurrentByteStoreBDB implements ByteStore {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentByteStoreBDB.class);

    private final SettingsJE settings;
    private final Environment bdb_env;
    private final DatabaseConfig bdb_cfg;
    private final Database bdb;

    private final File dir;
    private final AtomicLong gets = new AtomicLong(0);
    private final AtomicLong puts = new AtomicLong(0);
    private final AtomicLong bytesIn = new AtomicLong(0);
    private final AtomicLong bytesOut = new AtomicLong(0);
    private final HashSet<ClosableIterator<Map.Entry<byte[], byte[]>>> openIterators = new HashSet<>();
    private final LockMode lockMode = LockMode.DEFAULT;
    private final CursorConfig cursorConfig = CursorConfig.DEFAULT;
    private final OperationStatus opSuccess = OperationStatus.SUCCESS;

    public ConcurrentByteStoreBDB(File dir, String dbname) {
        this.dir = Files.initDirectory(dir);
        settings = new SettingsJE();
        EnvironmentConfig bdb_eco = new EnvironmentConfig();
        bdb_eco.setReadOnly(false);
        bdb_eco.setAllowCreate(true);
        bdb_eco.setTransactional(false);
        bdb_eco.setLockTimeout(2, TimeUnit.MINUTES);
        JEUtil.mergeSystemProperties(bdb_eco);
        SettingsJE.updateEnvironmentConfig(settings, bdb_eco);
        bdb_env = new Environment(dir, bdb_eco);
        bdb_cfg = new DatabaseConfig();
        bdb_cfg.setReadOnly(false);
        bdb_cfg.setAllowCreate(true);
        bdb_cfg.setDeferredWrite(true);
        SettingsJE.updateDatabaseConfig(settings, bdb_cfg);
        bdb = bdb_env.openDatabase(null, dbname, bdb_cfg);
        if (log.isDebugEnabled()) {
            log.debug(SettingsJE.dumpDebug(bdb));
        }
    }


    @Override
    public String toString() {
        return "BSBDB[" + gets + "," + puts + "]";
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean hasKey(byte[] key) {
        return bdb.get(null, new DatabaseEntry(key), new DatabaseEntry(), lockMode) == opSuccess;
    }

    @Override
    public byte[] firstKey() {
        return first(true);
    }

    @Override
    public byte[] firstEntry() {
        return first(false);
    }

    @Override
    public byte[] lastKey() {
        Cursor c = bdb.openCursor(null, cursorConfig);
        try {
            DatabaseEntry dk = new DatabaseEntry();
            if (c.getLast(dk, new DatabaseEntry(), lockMode) == opSuccess) {
                return dk.getData();
            }
            return null;
        } finally {
            c.close();
        }
    }

    @Override
    public void put(byte[] key, byte[] val) {
        if (bdb.put(null, new DatabaseEntry(key), new DatabaseEntry(val)) != opSuccess) {
            throw new RuntimeException("put fail");
        }
        bytesOut.addAndGet(key.length + val.length);
        puts.incrementAndGet();
    }

    @Override
    public byte[] get(byte[] key) {
        final DatabaseEntry dv = new DatabaseEntry();
        byte[] val;
        if (bdb.get(null, new DatabaseEntry(key), dv, lockMode) != opSuccess) {
            return null;
        }
        val = dv.getData();
        bytesIn.addAndGet(key.length + val.length);
        gets.incrementAndGet();
        return val;
    }

    @Override
    public byte[] higherKey(byte[] key) {
        final DatabaseEntry target = new DatabaseEntry(key);
        final DatabaseEntry dk = new DatabaseEntry(key);
        Cursor cursor = null;

        /** use partial entry to avoid pulling page data */
        final DatabaseEntry dvs = new DatabaseEntry();
        dvs.setPartial(0, 0, true);
        try {
            cursor = bdb.openCursor(null, cursorConfig);
            OperationStatus status = cursor.getSearchKeyRange(dk, dvs, lockMode);

            if (status != opSuccess) {
                return null;
            }

            int comparison = -1;
            while (status == opSuccess && (comparison = bdb.compareKeys(target, dk)) >= 0) {
                status = cursor.getNext(dk, dvs, lockMode);
            }
            if (status == opSuccess && comparison < 0) {
                byte[] rkey = dk.getData();
                gets.incrementAndGet();
                bytesIn.addAndGet(rkey.length);
                return rkey;
            }
            return null;
        } catch (EnvironmentFailureException e) {
            throw (e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public byte[] lowerKey(byte[] key) {
        final DatabaseEntry target = new DatabaseEntry(key);
        final DatabaseEntry dk = new DatabaseEntry(key);
        Cursor cursor = null;

        /** use partial entry to avoid pulling page data */
        final DatabaseEntry dvs = new DatabaseEntry();
        dvs.setPartial(0, 0, true);
        try {
            cursor = bdb.openCursor(null, cursorConfig);
            OperationStatus status = cursor.getSearchKeyRange(dk, dvs, lockMode);

            if (status != opSuccess) {
                status = cursor.getLast(dk, dvs, lockMode);
            }

            int comparison = -1;
            while (status == opSuccess && (comparison = bdb.compareKeys(target, dk)) <= 0) {
                status = cursor.getPrev(dk, dvs, lockMode);
            }
            if (status == opSuccess && comparison > 0) {
                byte[] rkey = dk.getData();
                gets.incrementAndGet();
                bytesIn.addAndGet(rkey.length);
                return rkey;
            }
            return null;
        } catch (EnvironmentFailureException e) {
            throw (e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public byte[] floorKey(byte[] key) {
        final DatabaseEntry target = new DatabaseEntry(key);
        final DatabaseEntry dk = new DatabaseEntry(key);
        Cursor cursor = null;

        /** use partial entry to avoid pulling page data */
        final DatabaseEntry dvs = new DatabaseEntry();
        dvs.setPartial(0, 0, true);
        try {
            cursor = bdb.openCursor(null, cursorConfig);
            OperationStatus status = cursor.getSearchKeyRange(dk, dvs, lockMode);

            if (status != opSuccess) {
                status = cursor.getLast(dk, dvs, lockMode);
            }

            int comparison = -1;
            while (status == opSuccess && (comparison = bdb.compareKeys(target, dk)) < 0) {
                status = cursor.getPrev(dk, dvs, lockMode);
            }
            if (status == opSuccess && comparison >= 0) {
                byte[] rkey = dk.getData();
                gets.incrementAndGet();
                bytesIn.addAndGet(rkey.length);
                return rkey;
            }
            return null;
        } catch (EnvironmentFailureException e) {
            throw (e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public Map.Entry<byte[], byte[]> floorEntry(byte[] key) {
        final DatabaseEntry target = new DatabaseEntry(key);
        final DatabaseEntry dk = new DatabaseEntry(key);
        final DatabaseEntry dv = new DatabaseEntry();
        Cursor cursor = null;

        /** use partial entry to avoid pulling page data */
        final DatabaseEntry dvs = new DatabaseEntry();
        dvs.setPartial(0, 0, true);

        try {
            cursor = bdb.openCursor(null, cursorConfig);
            OperationStatus status = cursor.getSearchKeyRange(dk, dvs, lockMode);

            if (status != opSuccess) {
                status = cursor.getLast(dk, dvs, lockMode);
            }

            int comparison = -1;
            while (status == opSuccess && (comparison = bdb.compareKeys(target, dk)) < 0) {
                status = cursor.getPrev(dk, dvs, lockMode);
            }
            if (status == opSuccess && comparison >= 0 && cursor.getCurrent(dk, dv, lockMode) == opSuccess) {
                byte[] rkey = dk.getData();
                byte[] rval = dv.getData();
                gets.incrementAndGet();
                bytesIn.addAndGet(rkey.length + rval.length);
                return new AbstractMap.SimpleImmutableEntry<>(rkey, rval);
            }
            return null;

        } catch (EnvironmentFailureException e) {
            throw (e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public Iterator<byte[]> keyIterator(final byte[] start) {
        final ClosableIterator<Map.Entry<byte[], byte[]>> entryIt = iterator(start, false, true);
        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return entryIt.hasNext();
            }

            @Override
            public byte[] next() {
                return entryIt.next().getKey();
            }

            @Override
            public void remove() {
                entryIt.remove();
            }
        };
    }

    private ClosableIterator<Map.Entry<byte[], byte[]>> iterator(final byte[] start, final boolean mustInclude,
            final boolean keyonly) {
        return new EntryClosableIterator(start, mustInclude, keyonly);
    }

    /**
     * internal/raw bdb delete
     */
    @Override public byte[] delete(byte[] key) throws DatabaseException {
        DatabaseEntry dk = new DatabaseEntry(key);
        DatabaseEntry dv = new DatabaseEntry();
        dv.setPartial(0, 0, true);
        Cursor cursor = bdb.openCursor(null, cursorConfig);
        try {
            if (cursor.getSearchKey(dk, dv, lockMode) == opSuccess &&
                cursor.delete() == opSuccess && cursor.getPrev(dk, dv, lockMode) == opSuccess) {
                return dk.getData();
            }
        } finally {
            cursor.close();
        }
        return null;
    }

    /**
     * return first key or value
     */
    private byte[] first(boolean key) {
        Cursor c = bdb.openCursor(null, cursorConfig);
        try {
            DatabaseEntry dk = new DatabaseEntry();
            DatabaseEntry dv = new DatabaseEntry();
            if (c.getFirst(dk, dv, lockMode) == opSuccess) {
                gets.incrementAndGet();
                bytesIn.addAndGet(dk.getSize() + dv.getSize());
                return key ? dk.getData() : dv.getData();
            }
            return null;
        } finally {
            c.close();
        }
    }

    @Override
    public void close() {
        close(false);
    }

    /**
     * Close the database.
     *
     * @param cleanLog if true then wait for the BerkeleyDB clean thread to finish.
     */
    @Override
    public void close(boolean cleanLog) {
        log.info("Closing db & env for: " + dir.getAbsolutePath());
        if (openIterators.size() > 0) {
            log.warn("closing " + openIterators.size() + " iterators on close");
            for (Object e : openIterators.toArray(new Object[openIterators.size()])) {
                ((ClosableIterator<Map.Entry>) e).close();
            }
        }
        log.info("pages:gets=" + gets + " puts=" + puts + " in=" + bytesIn + " out=" + bytesOut);

        bdb.close();
        if (cleanLog) {
            bdb_env.getConfig().setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
            int totalLogFiles = 0;
            int logFiles;
            do {
                logFiles = bdb_env.cleanLog();
                totalLogFiles += logFiles;
            }
            while (logFiles > 0);

            log.warn("Total of " + totalLogFiles + " log files cleaned.");

            if (totalLogFiles > 0) {
                CheckpointConfig force = new CheckpointConfig();
                force.setForce(true);
                bdb_env.checkpoint(force);
            }
        }
        bdb_env.close();
    }

    /**
     * This should be should be considered a fairly expensive operation.
     **/
    @Override
    public long count() {
        return bdb.count();
    }

    private class EntryClosableIterator implements ClosableIterator<Map.Entry<byte[], byte[]>> {

        private final DatabaseEntry dk;
        private final DatabaseEntry dv;
        private final DatabaseEntry dvs;
        private final byte[] start;
        private final boolean keyonly;
        private Map.Entry<byte[], byte[]> next;
        private Cursor cursor;

        public EntryClosableIterator(byte[] start, boolean mustInclude, boolean keyonly) {
            this.start = start;
            this.keyonly = keyonly;
            dk = new DatabaseEntry(start);
            dv = new DatabaseEntry();
            dvs = new DatabaseEntry();
            /** use partial entry to avoid pulling page data on a (likely) miss */
            dvs.setPartial(0, 0, true);
            try {
                cursor = bdb.openCursor(null, cursorConfig);
                OperationStatus status;
                boolean useAltKey = false;
                if (start == null || start.length == 0) {
                    status = cursor.getFirst(dk, dvs, lockMode);
                } else {
                    status = cursor.getSearchKeyRange(dk, dvs, lockMode);
                }
                if (status == opSuccess) {
                    if (log.isDebugEnabled()) {
                        log.debug("--> floor --> key=" + Bytes.toString(start) + " vs found=" + Bytes.toString(dk.getData()));
                    }
                    if (!Bytes.equals(start, dk.getData())) {
                        useAltKey = true;
                        status = cursor.getPrev(dk, dvs, lockMode);
                        if (log.isDebugEnabled()) log.debug("<-- prev -- " + status);
                    }
                } else {
                    status = cursor.getLast(dk, dvs, lockMode);
                    if (log.isDebugEnabled()) {
                        log.debug("--> floor --> tolast key=" + Bytes.toString(start) + " vs last=" + Bytes.toString(dk.getData()));
                    }
                }
                if (status == opSuccess || (useAltKey && !mustInclude)) {
                    if (!keyonly) {
                        cursor.getCurrent(dvs, dv, lockMode);
                    }
                    next = current();
                    if (log.isDebugEnabled()) log.debug("--> next key=" + Bytes.toString(next.getKey()));
                    synchronized (openIterators) {
                        openIterators.add(this);
                    }
                } else {
                    close();
                }
            } catch (EnvironmentFailureException e) {
                if (cursor != null) {
                    log.warn("Closing cursor");
                    cursor.close();
                }

                throw (e);
            }
        }

        @Override
        public String toString() {
            return "CI:" + Bytes.toString(dk.getData()) + "," + next + "," + cursor;
        }

        @Override
        protected void finalize() {
            close();
        }

        @Override
        public void close() {
            if (cursor != null) {
                cursor.close();
                cursor = null;
                synchronized (openIterators) {
                    openIterators.remove(this);
                }
            }
        }

        private Map.Entry<byte[], byte[]> current() {
            if (keyonly) {
                return new BytePageEntry(dk.getData());
            } else {
                return new BytePageEntry(dk.getData(), dv.getData());
            }
        }

        /**
         * Returns true if the iteration has more elements.
         * If the iteration has no more elements then
         * {@link EntryClosableIterator#close} will be invoked.
         *
         * @return true if the iteration has more elements.
         */
        @Override
        public boolean hasNext() {
            if (next == null && cursor != null) {
                OperationStatus status;
                if (keyonly) {
                    status = cursor.getNext(dk, dvs, lockMode);
                } else {
                    status = cursor.getNext(dk, dv, lockMode);
                }
                if (status == opSuccess) {
                    next = current();
                    if (log.isDebugEnabled()) log.debug("--  hasNext key=" + Bytes.toString(next.getKey()));
                } else {
                    close();
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
        public Map.Entry next() {
            if (hasNext()) {
                Map.Entry ret = next;
                if (log.isDebugEnabled()) log.debug("<-- next key=" + Bytes.toString(next.getKey()));
                next = null;
                return ret;
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            if (cursor.delete() != opSuccess) {
                throw new RuntimeException("unable to delete");
            }
        }
    }
}
