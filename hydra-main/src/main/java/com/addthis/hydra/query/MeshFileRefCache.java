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
package com.addthis.hydra.query;

import javax.annotation.Nullable;

import java.io.IOException;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.data.query.QueryException;
import com.addthis.meshy.ChannelCloseListener;
import com.addthis.meshy.MeshyConstants;
import com.addthis.meshy.MeshyServer;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.file.FileSource;

import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Collections2;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public class MeshFileRefCache implements ChannelCloseListener {

    private static final Logger log = LoggerFactory.getLogger(MeshFileRefCache.class);
    private static final int maxGetFileReferencesTime = Parameter.intValue("qmaster.maxListFilesTime", 60);

    // metrics
    private static final Timer fileReferenceFetchTimes = Metrics.newTimer(MeshFileRefCache.class, "fileReferenceFetchTimes", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private static final Counter fileReferenceFetches = Metrics.newCounter(MeshFileRefCache.class, "fileReferenceFetches");

    /**
     * Primary Mesh server
     */
    private final MeshyServer meshy;

    /**
     * Maintains a LRU cache of <code>FileReference</code> objects for a given job id.  If a Job
     * has 32 tasks then there should be 32 references in the set to indicate a correctly functioning job
     */
    private LoadingCache<String, Map<Integer, Set<FileReferenceWrapper>>> fileReferenceCache;

    /**
     * thread pool for refreshing cache keys asynchronously
     */
    private static final ExecutorService fileReferenceCacheReloader = MoreExecutors
            .getExitingExecutorService(new ThreadPoolExecutor(2, 5, 5000L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ThreadFactoryBuilder().setNameFormat("fileReferenceCacheReloader-%d").build()));

    //for testing
    public MeshFileRefCache() throws Exception {
        this.meshy = null;
        initializeFileReferenceCache();
    }

    public MeshFileRefCache(MeshyServer meshy) throws Exception {
        this.meshy = meshy;
        meshy.addChannelCloseListener(this);
        initializeFileReferenceCache();
    }

    public Map<Integer, Set<FileReferenceWrapper>> get(String job) throws ExecutionException {
        return fileReferenceCache.get(job);
    }

    public void invalidate(String job) {
        fileReferenceCache.invalidate(job);
    }

    private void initializeFileReferenceCache() {
        fileReferenceCache = CacheBuilder.newBuilder()
                .maximumSize(200)
                .refreshAfterWrite(2, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, Map<Integer, Set<FileReferenceWrapper>>>() {
                            public Map<Integer, Set<FileReferenceWrapper>> load(String prefix) throws IOException {
                                return loadFileReferencesForJob(prefix);
                            }

                            @Override
                            public ListenableFuture<Map<Integer, Set<FileReferenceWrapper>>> reload(final String key, Map<Integer, Set<FileReferenceWrapper>> oldValue) throws Exception {

                                ListenableFutureTask<Map<Integer, Set<FileReferenceWrapper>>> task = ListenableFutureTask.create(new Callable<Map<Integer, Set<FileReferenceWrapper>>>() {
                                    @Override
                                    public Map<Integer, Set<FileReferenceWrapper>> call() throws Exception {
                                        if (log.isTraceEnabled()) {
                                            log.trace("Refreshing file reference cache for: " + key);
                                        }
                                        return loadFileReferencesForJob(key);
                                    }
                                });
                                fileReferenceCacheReloader.submit(task);
                                return task;
                            }
                        });

        ScheduledExecutorService mqmFileRefCacheMaintainer = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder().setNameFormat("mqmFileRefCacheMaintainer=%d").build()));
        mqmFileRefCacheMaintainer.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                fileReferenceCache.cleanUp();
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    /**
     * Loads the file references for a given job.
     *
     * @param job - the UID of the job to get the FileReferences for
     * @return - a map of the 'best' file references for each task in the given job
     * @throws java.io.IOException
     */
    private Map<Integer, Set<FileReferenceWrapper>> loadFileReferencesForJob(String job) throws IOException {
        final long startTime = System.currentTimeMillis();
        fileReferenceFetches.inc();
        if (meshy.getChannelCount() == 0) {
            log.warn("[MeshQueryMaster] Error: there are no available mesh peers.");
            return null;
        }

        Map<Integer, Set<FileReferenceWrapper>> fileRefDataSet = getFileReferences(job, "*");
        if (log.isTraceEnabled()) {
            final StringBuilder sb = new StringBuilder();
            TreeMap<Integer, Set<FileReferenceWrapper>> sortedMap = new TreeMap<>(fileRefDataSet);
            for (Map.Entry<Integer, Set<FileReferenceWrapper>> mapEntry : sortedMap.entrySet()) {
                if (sb.length() > 0) {
                    sb.append("\n");
                }
                sb.append(mapEntry.getKey()).append(" has ").append(mapEntry.getValue().size()).append(" values");
            }
            log.trace("file reference details before filtering:\n " + sb);
        }
        fileRefDataSet = filterFileReferences(fileRefDataSet);

        if (log.isTraceEnabled()) {
            final StringBuilder sb = new StringBuilder();
            TreeMap<Integer, Set<FileReferenceWrapper>> sortedMap = new TreeMap<>(fileRefDataSet);
            for (Map.Entry<Integer, Set<FileReferenceWrapper>> mapEntry : sortedMap.entrySet()) {
                if (sb.length() > 0) {
                    sb.append("\n");
                }
                sb.append(mapEntry.getKey()).append(" has ").append(mapEntry.getValue().size()).append(" values");
            }
            log.trace("file reference details after filtering:\n" + sb);
        }

        long duration = System.currentTimeMillis() - startTime;
        if (log.isDebugEnabled()) {
            log.debug("File reference retrieval time: " + duration);
        }
        fileReferenceFetchTimes.update(duration, TimeUnit.MILLISECONDS);
        return fileRefDataSet;
    }

    /**
     * This method filters the file references to ensure that only valid file references are returned.
     * <p/>
     * The filter checks for two things.
     * <p/>
     * <ol>
     * <li>the last modified date for each file for the same task should be the same, if not it will take the
     * newest file</li>
     * <li>the size of the files should be equal, if not, take the files with the largest known size</li>
     * </ol>
     *
     * @param fileRefDataSet - the original unfiltered file reference set
     * @return - filtered file reference map containing only valid file references
     */
    protected static Map<Integer, Set<FileReferenceWrapper>> filterFileReferences(Map<Integer, Set<FileReferenceWrapper>> fileRefDataSet) {
        if (fileRefDataSet == null || fileRefDataSet.isEmpty()) {
            return null;
        }
        Map<Integer, Set<FileReferenceWrapper>> filteredFileReferenceSet = new HashMap<>(fileRefDataSet.size());
        for (Map.Entry<Integer, Set<FileReferenceWrapper>> entry : fileRefDataSet.entrySet()) {
            int key = entry.getKey();
            final Set<FileReferenceWrapper> fileReferenceWrappers = entry.getValue();
            long mostRecentTime = -1;

            for (FileReferenceWrapper fileReferenceWrapper : fileReferenceWrappers) {
                final FileReference fileReference = fileReferenceWrapper.fileReference;
                if (mostRecentTime < 0 || fileReference.lastModified > mostRecentTime) {
                    mostRecentTime = fileReference.lastModified;
                }
            }

            final long mostRecentTimeF = mostRecentTime;
            Predicate<FileReferenceWrapper> isMostRecent = new Predicate<FileReferenceWrapper>() {
                @Override
                public boolean apply(@Nullable FileReferenceWrapper input) {
                    return input != null && input.fileReference.lastModified == mostRecentTimeF;
                }
            };

            Collection<FileReferenceWrapper> filteredFileReferenceWrappers = Collections2.filter(fileReferenceWrappers, isMostRecent);
            filteredFileReferenceSet.put(key, new HashSet<>(filteredFileReferenceWrappers));

        }
        return filteredFileReferenceSet;
    }


    private Map<Integer, Set<FileReferenceWrapper>> getFileReferences(String job, String task) throws IOException {
        final String prefix = "*/" + job + "/" + task + "/gold/data/query";
        final Semaphore gate = new Semaphore(1);
        final ConcurrentHashMap<Integer, Set<FileReferenceWrapper>> fileRefMap = new ConcurrentHashMap<>();
        try {
            gate.acquire();
            new FileSource(meshy, MeshyConstants.LINK_NAMED, new String[]{prefix}) {
                @Override
                public void receiveReference(FileReference ref) {
                    String[] tokens = ref.name.split("/");
                    Integer id = Integer.parseInt(tokens[3]);
                    Set<FileReferenceWrapper> fileReferences = fileRefMap.putIfAbsent(id, new HashSet<FileReferenceWrapper>());
                    if (fileReferences == null) {
                        fileReferences = fileRefMap.get(id);
                    }
                    fileReferences.add(new FileReferenceWrapper(ref, id));
                }

                @Override
                public void receiveComplete() throws Exception {
                    gate.release();
                }
            };
            // wait until our HashMap to fill up to appropriate size
            gate.tryAcquire(maxGetFileReferencesTime, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            log.warn("Timed out waiting for mesh file list");
            throw new IOException("Timed out waiting for mesh file list");
        }
        if (log.isDebugEnabled()) {
            log.debug("found: " + fileRefMap.keySet().size() + " pairs");
        }
        return fileRefMap;
    }

    public FileReferenceWrapper getFileReferenceWrapperForSingleTask(String job, int taskId) throws IOException {
        Set<FileReferenceWrapper> refSet = getFileReferences(job, Integer.toString(taskId)).get(taskId);
        if (refSet == null || refSet.isEmpty()) {
            throw new QueryException("Could not find task reference for " + job + "/" + taskId);
        } else {
            return refSet.iterator().next();
        }

    }

    @Override
    public void channelClosed(int channelId) {
        // if a channel is closed then we need to invalidate
        // the fileReferenceCache so that we get new references
        // the next time a query is run
        if (log.isDebugEnabled()) {
            log.debug("[MeshQueryMaster] channel: " + channelId + " has been closed");
        }
        invalidateFileReferenceCache();
    }

    public synchronized void invalidateFileReferenceCache() {
        if (fileReferenceCache != null) {
            fileReferenceCache.invalidateAll();
        }
    }
}
