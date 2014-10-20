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

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.addthis.hydra.data.query.QueryException;
import com.addthis.meshy.ChannelCloseListener;
import com.addthis.meshy.MeshyServer;
import com.addthis.meshy.service.file.FileReference;

import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeshFileRefCache implements ChannelCloseListener {

    static final Logger log = LoggerFactory.getLogger(MeshFileRefCache.class);

    // metrics
    static final Timer fileReferenceFetchTimes = Metrics.newTimer(MeshFileRefCache.class, "fileReferenceFetchTimes", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    static final Counter fileReferenceFetches = Metrics.newCounter(MeshFileRefCache.class, "fileReferenceFetches");

    private final FileRefCacheLoader loader;

    /**
     * Maintains a LRU cache of {@code FileReference} objects for a given job id.  If a Job
     * has 32 tasks then there should be 32 references in the set to indicate a correctly functioning job
     */
    @Nonnull private final LoadingCache<String, Multimap<Integer, FileReference>> fileReferenceCache;

    public MeshFileRefCache(MeshyServer meshy) throws Exception {
        this.loader = new FileRefCacheLoader(meshy);
        this.fileReferenceCache = createLoadingCache(loader);
        meshy.addChannelCloseListener(this);
        startCacheMaintainer();
    }

    public Multimap<Integer, FileReference> get(String job) throws ExecutionException {
        return fileReferenceCache.get(job);
    }

    @Nonnull public Collection<FileReference> getTaskReferencesIfPresent(String job, int taskId) {
        Multimap<Integer, FileReference> refMap = fileReferenceCache.getIfPresent(job);
        if (refMap != null) {
            return refMap.asMap().get(taskId);
        }
        return Collections.emptySet();
    }

    public void invalidate(String job) {
        fileReferenceCache.invalidate(job);
    }

    private LoadingCache<String, Multimap<Integer, FileReference>> createLoadingCache(FileRefCacheLoader loader) {
        return CacheBuilder.newBuilder()
                           .maximumSize(200)
                           .refreshAfterWrite(2, TimeUnit.MINUTES)
                           .build(loader);
    }

    private void startCacheMaintainer() {
        ScheduledExecutorService mqmFileRefCacheMaintainer =
                new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("mqmFileRefCacheMaintainer=%d").build());
        mqmFileRefCacheMaintainer.scheduleAtFixedRate(fileReferenceCache::cleanUp, 30, 30, TimeUnit.SECONDS);
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
    @Nonnull
    protected static Multimap<Integer, FileReference> filterFileReferences(
            @Nonnull Multimap<Integer, FileReference> fileRefDataSet) {
        if (fileRefDataSet.isEmpty()) {
            return fileRefDataSet;
        }
        int baseKeySetSize = fileRefDataSet.keySet().size();
        Multimap<Integer, FileReference> filteredFileReferenceSet =
                HashMultimap.create(baseKeySetSize, fileRefDataSet.size() / baseKeySetSize);
        for (Map.Entry<Integer, Collection<FileReference>> entry : fileRefDataSet.asMap().entrySet()) {
            int key = entry.getKey();
            final Collection<FileReference> fileReferences = entry.getValue();
            long mostRecentTime = -1;

            for (FileReference fileReference : fileReferences) {
                if ((mostRecentTime < 0) || (fileReference.lastModified > mostRecentTime)) {
                    mostRecentTime = fileReference.lastModified;
                }
            }

            final long mostRecentTimeF = mostRecentTime;
            Predicate<FileReference> isMostRecent = input -> (input != null) && (input.lastModified == mostRecentTimeF);

            Collection<FileReference> filteredFileReferences = Collections2.filter(fileReferences, isMostRecent);
            filteredFileReferenceSet.putAll(key, filteredFileReferences);
        }
        return filteredFileReferenceSet;
    }

    public FileReference getFileReferenceForSingleTask(String job, int taskId) throws InterruptedException {
        Collection<FileReference> refSet = loader.getFileReferences(job, Integer.toString(taskId)).get(taskId);
        if ((refSet == null) || refSet.isEmpty()) {
            throw new QueryException("Could not find task reference for " + job + "/" + taskId);
        } else {
            return refSet.iterator().next();
        }
    }

    @Override
    public void channelClosed(int channelId) {
        // if a channel is closed then we need to invalidate the fileReferenceCache so
        // that we get new references the next time a query is run
        log.debug("[MeshQueryMaster] channel: {} has been closed", channelId);
        invalidateFileReferenceCache();
    }

    public void invalidateFileReferenceCache() {
        fileReferenceCache.invalidateAll();
    }

    public void updateFileReferenceForTask(String job, int task, Iterable<FileReference> baseSet) {
        Multimap<Integer, FileReference> existing = fileReferenceCache.getIfPresent(job);
        if (existing != null) {
            Multimap<Integer, FileReference> withReplacement = ImmutableMultimap.<Integer, FileReference>builder()
                                                                                .putAll(existing)
                                                                                .putAll(task, baseSet)
                                                                                .build();
            fileReferenceCache.put(job, withReplacement);
        }
    }

}
