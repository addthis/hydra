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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.addthis.basis.collect.HotMap;
import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.Parameter;

import com.addthis.hydra.store.kv.metrics.CachedPagedStoreMetrics;

import com.yammer.metrics.core.TimerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * basic page store that implements caching with simple eviction policy
 * and range iterators that provide some protection against concurrent modification
 * of underlying pages.
 * <p/>
 * contract requires that a page's first key never changes once a page is created
 *
 * @param <K>
 * @param <V>
 *         <p/>
 *         TODO enable single CacheEvictor for multi-db environments
 */
public abstract class CachedPagedStore<K, V> implements PagedKeyValueStore<K, V> {

    private static final boolean collectMetrics = Parameter.boolValue("eps.debug.collect", false);
    private static final Logger log = LoggerFactory.getLogger(CachedPagedStore.class);

    private static final HotMap<CachedPagedStore, CachedPagedStore> hotCache = new HotMap<>(new HashMap());
    protected final TreeMap<K, KeyValuePage<K, V>> cache = new TreeMap<>(); // TODO revert to private after removing debugging code
    private final HotMap<K, K> recentPageKeys = new HotMap<>(new HashMap<K, K>());
    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(); // TODO revert to private after removing debugging code
    private final CacheEvictor cacheCleaner = new CacheEvictor();
    private final boolean debugLocks = Parameter.boolValue("cps.locks.debug", false);
    private final int evictBatch = Parameter.intValue("cps.evict.batch", 10);
    protected final AtomicInteger pageGets = new AtomicInteger(0); // TODO revert to private after removing debugging code

    private final CachedPagedStoreMetrics metrics = collectMetrics ? new CachedPagedStoreMetrics() : null;

    public static final void setDebugLocks(boolean debug) {
        System.setProperty("cps.locks.debug", debug ? "1" : "0");
    }

    /**
     * @return true if the underlying storage is read only
     */
    public abstract boolean isReadOnly();

    /**
     * return a page that either contains or could uniquely contain the given key,
     * creating the page if necessary.  page and associated starting key is pinned
     * and cannot be deleted from the backing store until releasePage() is called.
     *
     * @param key key that page must contain or be able to contain
     * @return matching page
     */
    public abstract KeyValuePage<K, V> fetchPage(K key);

    /**
     * activate a lease on a fetched page.  must be called at most
     * once for each fetched page before being released.
     */
    public abstract void leasePage(KeyValuePage<K, V> page);

    /**
     * called when a page is no longer referenced and can be returned to backing store.
     *
     * @see #fetchPage
     */
    public abstract void releasePage(KeyValuePage<K, V> page);

    /**
     * return a lexicographically ordered set of pages
     *
     * @param start key to match starting page or null for all pages
     * @return
     */
    public abstract Iterator<Entry<K, KeyValuePage<K, V>>> getPageIterator(K start);

    /**
     * @return true if the cache should evict pages to free resources
     */
    public abstract boolean shouldEvictPage();

    /**
     * @return true if the cache should evict pages to free resources
     */
    public abstract boolean mustEvictPage();

    /**
     * @return size in number of pages of the current memory cache
     */
    public int getCachedEntryCount() {
        return recentPageKeys.size();
    }

    private final boolean tryWriteLock() {
        return lock.writeLock().tryLock();
    }

    private final void writeLock() {
        lock.writeLock().lock();
    }

    private final void writeUnlock() {
        lock.writeLock().unlock();
    }

    private void requireWriteLock() {
        if (debugLocks && !lock.writeLock().isHeldByCurrentThread()) {
            throw new RuntimeException("write lock must be held for this operation");
        }
    }

    /**
     * cleaner usually only runs in read-write environments.  when the
     * cleaner thread is not running, all cleaning is forced at point of
     * page fetch.  this is better for read-only environments.
     */
    public void startCacheCleaner() {
        cacheCleaner.start();
    }

    /**
     * @param key page index key (first key)
     * @return matching page if it exists in the cache
     */
    public KeyValuePage<K, V> getCachedPage(K key) {
        lock.readLock().lock();
        try {
            return cache.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * utility method for subclasses to inject pages into cache.
     * best example is on split, subclass can inject new empty
     * page into cache to save next fetch.
     * <p/>
     * critical that this only be called with writelock held,
     * which is the case inside all methods that use getPage().
     * <p/>
     * usually this is an artifact of a call to page reValidate()
     * which in turn calls split().
     * <p/>
     * called from releasePage() on a page delete or during split()
     *
     * @param page
     */
    protected void putPageIntoCache(KeyValuePage<K, V> page) {
        requireWriteLock();
        K key = page.getFirstKey();
        if (cache.put(key, page) == null) {
            leasePage(page);
        }
        recentPageKeys.put(key, key);
        if (log.isDebugEnabled()) log.debug("putPageIntoCache " + page);
    }

    private KeyValuePage<K, V> doFetchPage(K key) {
        TimerContext timerContext = null;
        if (collectMetrics) {
            timerContext = metrics.startTimer();
        }
        KeyValuePage<K, V> page = fetchPage(key);
        if (collectMetrics) {
            timerContext.stop();
        }
        return page;
    }

    /**
     * locates or creates required page.
     */
    private KeyValuePage<K, V> getPage(K key) {
        pageGets.incrementAndGet();
        requireWriteLock();
        KeyValuePage<K, V> page;
        boolean first = true;
        /**
         * this loop allows pages to be split and always return the
         * right answer (a page that does or can uniquely contain a key)
         */
        do {
            boolean cacheHit = false;
            Entry<K, KeyValuePage<K, V>> pageEntry = cache.floorEntry(key);
            if (pageEntry == null) {
                page = doFetchPage(key);
                if (cache.put(page.getFirstKey(), page) == null) {
                    leasePage(page);
                }
            } else {
                page = pageEntry.getValue();
                if (!page.mayContainKey(key)) {
                    page = doFetchPage(key);
                    if (cache.put(page.getFirstKey(), page) == null) {
                        leasePage(page);
                    }
                } else {
                    cacheHit = true;
                }
            }
            if (collectMetrics && first) {
                metrics.recordPageCache(cacheHit);
            }
            if (log.isDebugEnabled()) log.debug("getPage " + key + "=" + page + " cache=" + cache);
            cacheCleaner.updateRecentList(page.getFirstKey());
            first = false;
        }
        while (page.reValidate());
        return page;
    }

    @Override
    public V getValue(K key) {
        V ret = null;
        PageWrapper pageWrapper = getPageWrapper(key);
        try {
            ret = pageWrapper.getPage().getValue(key);
        } finally {
            pageWrapper.getPin().release();
        }
        return ret;
    }

    @Override
    public V getPutValue(K key, V val) {
        V ret = null;
        PageWrapper pageWrapper = getPageWrapper(key);
        try {
            ret = pageWrapper.getPage().getPutValue(key, val);
        } finally {
            pageWrapper.getPin().release();
        }
        return ret;
    }

    @Override
    public V getRemoveValue(K key) {
        V ret = null;
        PageWrapper pageWrapper = getPageWrapper(key);
        try {
            ret = pageWrapper.getPage().getRemoveValue(key);
        } finally {
            pageWrapper.getPin().release();
        }
        return ret;
    }

    @Override
    public void putValue(K key, V val) {
        PageWrapper pageWrapper = getPageWrapper(key);
        try {
            pageWrapper.getPage().putValue(key, val);
        } finally {
            pageWrapper.getPin().release();
        }
    }

    @Override
    public void removeValue(K key) {
        PageWrapper pageWrapper = getPageWrapper(key);
        try {
            pageWrapper.getPage().removeValue(key);
        } finally {
            pageWrapper.getPin().release();
        }
    }

    @Override
    public void removeValues(K start, K end, boolean inclusive) {
        throw new UnsupportedOperationException();
    }

    private PageWrapper getPageWrapper(K key) {
        PageWrapper pageWrapper = new PageWrapper();
        writeLock();
        try {
            pageWrapper.setPage(getPage(key));
        } finally {
            writeUnlock();
        }
        return pageWrapper;
    }

    @Override
    public Iterator<Entry<K, V>> range(K start, boolean inclusive) {
        return new BoundedIterator(getPageIterator(start), start, inclusive);
    }

    /**
     * flush cache, close
     */
    @Override
    public void close() {
        if (log.isDebugEnabled()) log.debug("closing " + this);
        try {
            cacheCleaner.terminate();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        if (log.isDebugEnabled()) log.debug("close releasing " + cache);
        /**
         * TODO possible concurrent mod error if release of
         * empty page causes a cache put b/c the previous
         * page pointers were updated.
         */
        for (KeyValuePage<K, V> e : cache.values()) {
            releasePage(e);
        }
    }

    private K evictOne() {
        requireWriteLock();
        K nextKey = recentPageKeys.removeEldest();
        // can happen if should evict always returns true
        if (nextKey == null) {
            return null;
        }
        // skip pages that are not in cache
        KeyValuePage<K, V> candidate = cache.remove(nextKey);
        if (candidate == null) {
            if (log.isErrorEnabled()) log.error("evictOne SKIP null candidate " + nextKey + " hot=" + recentPageKeys.size());
            return null;
        }
        // return key to cache, hot and queue if pinned
        if (candidate.isPinned()) {
            if (log.isDebugEnabled()) log.debug("evictOne SKIP pinned candidate " + nextKey);
            recentPageKeys.put(nextKey, nextKey);
            cache.put(nextKey, candidate);
            return null;
        }
        releasePage(candidate);
        return nextKey;
    }

    /**
     * wrapper for page iterator
     */
    private class BoundedIterator implements ClosableIterator<Entry<K, V>> {

        private K firstKey;
        private boolean inclusive;
        private Iterator<Entry<K, KeyValuePage<K, V>>> rangeIterator;
        private Iterator<Entry<K, V>> pageIterator;
        private Entry<K, KeyValuePage<K, V>> nextPage;
        private Entry<K, V> lastEntry;
        private Entry<K, V> nextEntry;

        BoundedIterator(Iterator<Entry<K, KeyValuePage<K, V>>> iterator, K firstKey, boolean inclusive) {
            this.rangeIterator = iterator;
            this.firstKey = firstKey;
            this.inclusive = inclusive;
            while (hasNext() && firstKey != null && compareKeys(nextEntry.getKey(), firstKey) < 0) {
                next();
            }
            if (!inclusive) {
                next();
            }
            if (log.isDebugEnabled()) log.debug("BI:firstKey=" + firstKey + ",inc=" + inclusive + ",next=" + nextEntry);
        }

        @Override
        public String toString() {
            return "BI:" + firstKey + "," + inclusive + "," + rangeIterator + "," + pageIterator + "," + nextPage + "," + lastEntry + "," + nextEntry;
        }

        private void fillNext() {
            /* first make sure we have a viable page */
            while (pageIterator == null && rangeIterator != null && rangeIterator.hasNext()) {
                nextPage = rangeIterator.next();
                pageIterator = nextPage.getValue().range(firstKey, inclusive);
                if (!pageIterator.hasNext()) {
                    pageIterator = null;
                }
            }
            /* make sure we have a viable page iterator */
            if (nextEntry == null && pageIterator != null && pageIterator.hasNext()) {
                nextEntry = pageIterator.next();
                if (!pageIterator.hasNext()) {
                    pageIterator = null;
                    nextPage = null;
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("BI:fillNext,pi=" + pageIterator + ",ri=" + rangeIterator + ",np=" + (nextPage != null ? nextPage.getKey() : null) + ",ne=" + (nextEntry != null ? nextEntry.getKey() : null));
            }
        }

        @Override
        public void close() {
            if (pageIterator != null && pageIterator instanceof ClosableIterator) {
                ((ClosableIterator) pageIterator).close();
            }
            if (rangeIterator != null && rangeIterator instanceof ClosableIterator) {
                ((ClosableIterator) rangeIterator).close();
            }
            rangeIterator = null;
            pageIterator = null;
        }

        @Override
        public boolean hasNext() {
            fillNext();
            return nextEntry != null;
        }

        @Override
        public Entry<K, V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            lastEntry = nextEntry;
            nextEntry = null;
            return lastEntry;
        }

        @Override
        public void remove() {
            if (nextPage != null && lastEntry != null) {
                nextPage.getValue().getRemoveValue(lastEntry.getKey());
            } else {
                throw new NoSuchElementException();
            }
        }

    }


    /**
     * a tuple of a page and its pin
     */
    private class PageWrapper {

        PagePin pin = null;
        KeyValuePage<K, V> page = null;

        public PagePin getPin() {
            return pin;
        }

        public KeyValuePage<K, V> getPage() {
            return page;
        }

        public void setPage(KeyValuePage<K, V> page) {
            this.page = page;
            this.pin = page.pin();
        }
    }

    /**
     * background daemon that tries to keep the cache clean
     * ahead of put activity.
     */
    private class CacheEvictor implements Runnable {

        private final AtomicBoolean terminated = new AtomicBoolean(false);
        private Thread thread;

        public void start() {
            if (thread == null) {
                thread = new Thread(this, "CacheEvictor");
                thread.setDaemon(true);
                thread.start();
            }
        }

        /**
         * stop and join evictor thread.
         * evict all remaining entries from cache.
         *
         * @throws InterruptedException
         */
        private void terminate() throws InterruptedException {
            terminated.set(true);
            if (thread != null) {
                thread.join();
                thread = null;
            }
            evictAll();
        }

        private void evictAll() {
            writeLock();
            try {
                int cacheSize = cache.size();
                int attempts = cacheSize * 2;
                while (attempts-- > 0 && evictOne() != null) {
                    ;
                }
                if (attempts == 0 && cache.size() > 0) {
                    log.warn("evictor failed to empty cache on terminate. " + cache.size() + " remain");
                }
                if (log.isDebugEnabled()) log.debug("evictAll start=" + cacheSize + " end=" + cache.size());
            } finally {
                writeUnlock();
            }
        }

        /**
         * either brings cache into strict compliance or notifies cache that
         * it should look for things to clean.
         *
         * @param hotKey optional parameter that causes evictor loop to terminate
         */
        private void updateRecentList(final K hotKey) {
            recentPageKeys.put(hotKey, hotKey);
//          if (thread == null)
//          {
//              synchronized (hotCache)
//              {
//                  hotCache.put(CachedPagedStore.this, CachedPagedStore.this);
//              }
//              if (mustEvictPage())
//              {
//                  CachedPagedStore store = null;
//                  synchronized (hotCache)
//                  {
//                      store = hotCache.peekEldest();
//                  }
//                  if (store != CachedPagedStore.this && store.tryWriteLock())
//                  {
//                      try
//                      {
//                          while (mustEvictPage())
//                          {
//                              Object evicted = store.evictOne();
//                              if (evicted == null || evicted == hotKey)
//                              {
//                                  synchronized (hotCache)
//                                  {
//                                      hotCache.removeEldest();
//                                  }
//                                  return;
//                              }
//                          }
//                      }
//                      finally
//                      {
//                          store.writeUnlock();
//                      }
//                  }
//              }
//          }
            while (mustEvictPage()) {
                if (evictOne() == hotKey) {
                    throw new RuntimeException("evicted hottest key " + hotKey);
                }
            }
        }

        public void run() {
            long waitTime = 2;
            while (!terminated.get()) {
                try {
                    while (!shouldEvictPage()) {
                        if (terminated.get()) {
                            return;
                        }
                        synchronized (this) {
                            this.wait(waitTime);
                        }
                        waitTime = Math.min(waitTime * 2, 64);
                    }
                    waitTime = 2;
                    writeLock();
                    try {
                        int evicted = evictBatch;
                        /** drain queue and avoid spinning on pinned pages (wrap around to firstKey) */
                        while (shouldEvictPage()) {
                            if (terminated.get()) {
                                return;
                            }
                            evictOne();
                            /** exit on excessive clean attempts */
                            if (--evicted <= 0 && !mustEvictPage()) {
                                break;
                            }
                        }
                    } finally {
                        writeUnlock();
                    }
                } catch (Error e)  {
                    log.warn("", e);
                    throw e;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public abstract void setMaxPages(int maxPages);

    @Override
    public abstract void setMaxPageSize(int maxPageSize);

    @Override
    public abstract void setMaxPageMem(long maxPageMem);

    @Override
    public abstract void setMaxTotalMem(long maxTotalMem);

    @Override
    public abstract void setMemEstimateInterval(int interval);

}
