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
package com.addthis.hydra.task.stream;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.util.Strings;

import com.addthis.codec.annotations.Bytes;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.annotations.Time;
import com.addthis.hydra.task.source.AbstractPersistentStreamSource;
import com.addthis.maljson.JSONObject;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.file.FileSource;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Objects.toStringHelper;

/**
 * <p>Specifies a set of files to fetch from the meshy server. The files are filtered based on a range of dates.</p>
 * <p/>
 * <p>The {@link #startDate startDate} and {@link #endDate endDate} fields can be formatted using
 * the <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html">DateTimeFormat</a>
 * represented by the {@link #dateFormat dateFormat} field. Or, the startDate and endDate
 * can be of the form {{now}}, {{now+N}}, or {{now-N}}. {{now}} returns the current date and time,
 * {{now+N}} returns N days ahead of the current date and time, and {{now-N}} returns N days behind
 * the current date and time.
 * </p>
 * <p>A suffix of the input file names can be used for sorting the file names and
 * generating the path names. The suffix will first eliminate a pre-specified number of
 * sortToken occurrences and then eliminate another pre-specified
 * number of characters. The default value of sortToken is "/". The suffix for sorting starts at
 * the beginning of the input file name and skips
 * over the a sortTokenOffset number of sortToken characters.
 * Next a fixed length of sortOffset characters are skipped over.
 * The following three examples assume the file name "/ab/def/ghij/jklmno/pqr"
 * <table>
 * <tr>
 * <th>sortTokenOffset</th>
 * <th>sortOffset</th>
 * <th>Suffix</th>
 * </tr>
 * <tr>
 * <td>2</td>
 * <td>0</td>
 * <td>def/ghij/jklmno/pqr</td>
 * </tr>
 * <tr>
 * <td>0</td>
 * <td>3</td>
 * <td>/def/ghij/jklmno/pqr</td>
 * </tr>
 * <tr>
 * <td>2</td>
 * <td>3</td>
 * <td>f/ghij/jklmno/pqr</td>
 * </tr>
 * </table>
 * <p>The default values for the sortTokenOffset and pathTokenOffset fields are based on
 * the file path <code>/job/<job>/<task>/gold/split/{Y}{M}{D}</code></p>
 * <p/>
 * <p>Example:</p>
 * <pre>mesh : {
 *     startDate : "%[start-date:{{now-2}}]%",
 *     endDate : "%[end-date:{{now}}]%",
 *     meshPort : "%[meshPort:5500]%",dateFormat:"YYMMddHH",
 *     files : ["/job/%[hoover-job]%/&#42;/gold/hoover.out/{Y}{M}{D}/{H}/{{mod}}-&#42;"],
 * },</pre>
 *
 * @user-reference
 */
public class StreamSourceMeshy extends AbstractPersistentStreamSource {

    private static final Logger log = LoggerFactory.getLogger(StreamSourceMeshy.class);

    private static final Splitter HOST_METADATA_SPLITTER = Splitter.on(',').omitEmptyStrings();

    /**
     * the percentage of peers that must respond before the finder returns
     */
    @FieldConfig(codable = true)
    private double meshPeerThreshold;

    /**
     * if true the finder may return before 100% of peers have responded
     */
    @FieldConfig(codable = true)
    private boolean meshShortCircuit;

    /**
     * the amount of time the mesh finder will wait before potentially returning.
     * Given in either milliseconds or a string with a number and a unit like "5 seconds"
     */
    @Time(TimeUnit.MILLISECONDS)
    @FieldConfig(codable = true)
    private int meshTimeOut;

    /**
     * Hostname of the meshy server. Default is either "source.mesh.host" configuration value or
     * "localhost".
     */
    @FieldConfig(codable = true)
    private String meshHost;

    /**
     * Port number of the meshy server. Default is either "source.mesh.port" configuration value
     * or 5000.
     */
    @FieldConfig(codable = true)
    private int meshPort;

    /**
     * Default mesh node cache per stream in size of bytes or appened with standard human readable
     * units like "56k".
     */
    @Bytes
    @FieldConfig(codable = true)
    int meshStreamCache;

    @FieldConfig(codable = true)
    private int maxRangeDays;

    @FieldConfig(codable = true)
    private int maxRangeHours;

    /**
     * Length of time to wait before possibly short circuiting mesh lookups.
     * Given in either milliseconds or a string with a number and a unit like "5 seconds".
     */
    @Time(TimeUnit.MILLISECONDS)
    @FieldConfig(codable = true)
    private int meshShortCircuitWaitTime;

    private static final ConcurrentHashMultiset<String> lateFileFindMap = ConcurrentHashMultiset.create();

    private final Map<String, List<MeshyStreamFile>> cacheMap       = new HashMap<>();
    private final LinkedList<String>                 cache          = new LinkedList<>();
    private final Object                             nextSourceLock = new Object();

    MeshyClient meshLink;
    private MeshHostScoreCache scoreCache;
    DateTime firstDate;
    DateTime lastDate;

    private volatile int peerCount = -1;

    @Override
    protected boolean doInit() throws IOException {
        /* establish link to the mesh */
        meshLink = new MeshyClient(meshHost, meshPort);
        log.info("mesh connected to {}:{}", meshHost, meshPort);
        scoreCache = new MeshHostScoreCache(meshLink);
        return true;
    }

    boolean useProcessedTimeRangeMax() {
        return (maxRangeDays + maxRangeHours) > 0;
    }

    /** query the mesh for a list of matching files */
    private List<MeshyStreamFile> findMeshFiles(final DateTime date, String[] patterns) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("find using mesh={} patterns={}", meshLink, Arrays.toString(patterns));
        }
        // responding peer count should only be mutated by meshy threads
        final AtomicInteger respondingPeerCount = new AtomicInteger();
        final Semaphore gate = new Semaphore(1);
        final ConcurrentHashMap<String, Histogram> responseTimeMap = new ConcurrentHashMap<>();

        final List<MeshyStreamFile> fileReferences = new ArrayList<>(100 * patterns.length);
        try {
            gate.acquire();
        } catch (InterruptedException e) {
            throw new IOException("interrupted while waiting for gate");
        }
        final long startTime = System.currentTimeMillis();
        final AtomicBoolean shortCircuited = new AtomicBoolean(false);
        peerCount = -1;
        FileSource source = new FileSource(meshLink) {
            // both must be initialized in 'peers' to make sense elsewhere; TODO: add explicit state
            boolean localMeshFindRunning;
            Set<String> unfinishedHosts; // purely cosmetic

            @Override
            public void receiveReference(FileReference ref) {
                String name = ref.name;
                if (name.charAt(0) != '/') {
                    switch (name) {
                        case "peers":
                            // the host uuid is a list of remote peers who were sent requests
                            unfinishedHosts =
                                    Sets.newHashSet(
                                            HOST_METADATA_SPLITTER.split(ref.getHostUUID()));
                            unfinishedHosts.add(meshHost);
                            // include the local mesh node
                            peerCount = (int) ref.size + 1;
                            localMeshFindRunning = true;
                            StreamSourceMeshy.log.debug(toLogString("init"));
                            return;
                        case "response":
                            unfinishedHosts.remove(ref.getHostUUID());
                            // ref.size is the number of outstanding remote requests
                            int outstandingRequests = (int) ref.size;
                            // adjust for a possibly outstanding local request
                            if (localMeshFindRunning) {
                                outstandingRequests += 1;
                            }
                            int newCompleteResponsesCount = peerCount - outstandingRequests;
                            // information is allowed to be forwarded out of order so take maximum
                            respondingPeerCount.set(
                                    Math.max(newCompleteResponsesCount, respondingPeerCount.get()));
                            StreamSourceMeshy.log.debug(toLogString("response"));
                            return;
                        case "localfind":
                            localMeshFindRunning = false;
                            unfinishedHosts.remove(meshHost);
                            respondingPeerCount.incrementAndGet();
                            StreamSourceMeshy.log.debug(toLogString("localfind"));
                            return;
                        default:
                            StreamSourceMeshy.log.warn(
                                    "Found a file ref without a prepended /. Assuming its a real " +
                                    "fileref for now : {}",
                                    ref.name);
                    }
                }
                String hostId = ref.getHostUUID().substring(0, ref.getHostUUID().indexOf("-"));
                if (shortCircuited.get()) {
                    lateFileFindMap.add(hostId);
                    // we are done here
                    return;
                }
                long receiveTime = System.currentTimeMillis();
                fileReferences.add(new MeshyStreamFile(StreamSourceMeshy.this, date, ref));
                if (responseTimeMap.containsKey(hostId)) {
                    Histogram histo = responseTimeMap.get(hostId);
                    histo.update((receiveTime - startTime));
                } else {
                    Histogram histo = Metrics.newHistogram(StreamSourceMeshy.class, hostId + "refResponseTime.JMXONLY");
                    responseTimeMap.put(hostId, histo);
                    histo.update((receiveTime - startTime));
                }
            }

            // called when all mesh nodes have completed. In this case we have only one mesh node who should only
            //   trigger this event when all the remote mesh nodes have completed
            @Override
            public void receiveComplete() throws Exception {
                StreamSourceMeshy.log.debug(toLogString("all-complete"));
                gate.release();
            }

            @Override
            public String toString() {
                return toLogString("FileRefSource");
            }

            private String toLogString(String reason) {
                return toStringHelper(reason)
                        .add("peer-count", peerCount)
                        .add("responses", respondingPeerCount.get())
                        .add("run-time", System.currentTimeMillis() - startTime)
                        .add("waiting", unfinishedHosts)
                        .toString();
            }
        };
        source.requestRemoteFilesWithUpdates(patterns);
        while (true) {
            try {
                if (!gate.tryAcquire(meshShortCircuitWaitTime, TimeUnit.MILLISECONDS)) {
                    if (meshShortCircuit
                        && peerCount > 0
                        && (System.currentTimeMillis() - startTime) > meshTimeOut
                        && respondingPeerCount.get() > (meshPeerThreshold * peerCount)) {
                        // break early
                        shortCircuited.set(true);
                        log.warn("Breaking after receiving responses from {} of {} peers",
                                 respondingPeerCount.get(), peerCount);
                        break;
                    } else {
                        try {
                            log.warn(source.toString());
                        } catch (ConcurrentModificationException cme) {
                            //then we must be at least making progress so whatever
                        }
                    }
                } else {
                    // got the lock, all set!
                    break;
                }
            } catch (InterruptedException e) {
            }
        }
        if (log.isTraceEnabled()) {
            double max = -1.0d;
            String slowHost = "";
            for (Map.Entry<String, Histogram> entry : responseTimeMap.entrySet()) {
                log.trace("hostTime: " + entry.getKey() + " - " + entry.getValue().max());
                if (entry.getValue().max() > max) {
                    slowHost = entry.getKey() + " - " + entry.getValue().max();
                    max = entry.getValue().max();
                }
            }
            log.trace("\nslowHost: " + slowHost);
        }
        return fileReferences;
    }

    @Override
    public StreamFile nextSource() {
        MeshyStreamFile next = selectMeshHost(getOrLoadNextHosts());
        if ((firstDate != null) && useProcessedTimeRangeMax()) {
            long range = next.date.getMillis() - firstDate.getMillis();
            if ((maxRangeDays > 0) && (range > (ONE_DAY_IN_MILLIS * maxRangeDays))) {
                log.warn("truncating source list. over max days: {}", maxRangeDays);
                moreData = true;
                return null;
            }
            if ((maxRangeHours > 0) && (range > (ONE_HOUR_IN_MILLIS * maxRangeHours))) {
                log.warn("truncating source list. over max hours: {}", maxRangeHours);
                moreData = true;
                return null;
            }
        }
        return next;
    }

    private List<MeshyStreamFile> getOrLoadNextHosts() {
        synchronized (nextSourceLock) {
            while (cache.isEmpty() && !dates.isEmpty()) {
                try {
                    fillCache(dates.removeFirst());
                } catch (Exception ex) {
                    log.warn("", ex);
                    return null;
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("@ next source dates={} cache={} peek={} map={}",
                          dates.size(), cache.size(), cache.peekFirst(), cacheMap.get(cache.peekFirst()));
            }
            if (cache.isEmpty()) {
                return null;
            }
            String cacheKey = reverse ? cache.removeLast() : cache.removeFirst();
            return cacheMap.remove(cacheKey);
        }
    }

    /**
     * Filters a list of MeshyStreamFiles, gets score values for each of them, and performs an inverse lottery
     * selection.
     */
    private MeshyStreamFile selectMeshHost(List<MeshyStreamFile> sourceCandidates) {
        if ((sourceCandidates == null) || sourceCandidates.isEmpty()) {
            return null;
        }
        if (sourceCandidates.size() == 1) {
            return sourceCandidates.get(0);
        }
        List<MeshyStreamFile> filteredCandidates = filterFiles(sourceCandidates);
        int[] scores = new int[filteredCandidates.size()];
        int idx = 0;
        for (MeshyStreamFile file : filteredCandidates) {
            int score = scoreCache.get(file.meshFile.getHostUUID());
            scores[idx] = score;
            idx += 1;
        }
        int selection = inverseLotterySelection(scores);
        return filteredCandidates.get(selection);
    }

    /**
     * Filters the candidate file sources by using only the most up to date file
     */
    private List<MeshyStreamFile> filterFiles(List<MeshyStreamFile> sourceCandidates) {
        DateTime mostRecent = sourceCandidates.get(0).date;
        long longest = 0;
        for (MeshyStreamFile file : sourceCandidates) {
            if (file.date.isAfter(mostRecent)) {
                mostRecent = file.date;
            }
            if (file.length() > longest) {
                longest = file.length();
            }
        }
        Iterator<MeshyStreamFile> iterator = sourceCandidates.iterator();
        while (iterator.hasNext()) {
            MeshyStreamFile file = iterator.next();
            if (file.date.isBefore(mostRecent) || (file.length() < longest)) {
                log.warn("Ignoring a file choice from {} because it was behind." +
                         "\nThis is expected to happen rarely under normal conditions.", file.meshFile.getHostUUID());
                 // remove ops on a List aren't super efficient but this is not supposed to be common
                iterator.remove();
            }
        }
        return sourceCandidates;
    }

    /**
     * Performs an inverse lottery selection based on the scores provided. Higher scores mean you are less
     * likely to be selected. Each point in your score gives a ticket to everyone else.
     */
    private static int inverseLotterySelection(int... scores) {
        int totalScore = 0;
        for (int i: scores) {
            totalScore += i;
        }
        int adjustedTotalScore = totalScore * (scores.length - 1);
        int selection = (int) (adjustedTotalScore * Math.random());
        for (int i = 0; i < scores.length; i++) {
            int adjustedScore = totalScore - scores[i];
            adjustedTotalScore -= adjustedScore;
            if (selection >= adjustedTotalScore) {
                return i;
            }
        }
        log.warn("There was an error in lottery selection; defaulting to the first choice.");
        return 0;
    }

    /**
     * Fills the cache and cacheMap with paths and mappings of those paths to mesh host options
     * The cache is an ordered list which reflects the ordering specified by the sort offset.
     */
    private void fillCache(DateTime timeToLoad) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("filling cache for date " + timeToLoad);
        }

        long start = System.currentTimeMillis();
        final List<MeshyStreamFile> streamSources = getStreamSources(timeToLoad, getDateTemplatedFileList(timeToLoad));
        for (MeshyStreamFile streamSource : streamSources) {
            if (log.isDebugEnabled()) {
                log.debug("cache added " + streamSource);
            }
            String cacheKey = streamSource.getPath();
            List<MeshyStreamFile> sourceOptions = cacheMap.get(cacheKey);
            if (sourceOptions == null) {
                sourceOptions = new ArrayList<>(2);
                cacheMap.put(cacheKey, sourceOptions);
                cache.add(cacheKey);
            }
            sourceOptions.add(streamSource);
        }
        long end = System.currentTimeMillis();
        log.info(toStringHelper("File reference cache fill")
                .add("date", formatter.print(timeToLoad))
                .add("files-found", streamSources.size())
                .add("after-filtering", cache.size())
                .add("fills-left", dates.size())
                .add("fill-time", (end - start))
                .toString());
    }

    /** */
    private List<MeshyStreamFile> getStreamSources(DateTime date, String[] matches) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("getStreamSources :: " + date + " :: " + Strings.join(matches, " -- "));
        }
        List<MeshyStreamFile> streamSources = findMeshFiles(date, matches);
        // sort files
        Collections.sort(streamSources, new Comparator<MeshyStreamFile>() {
            @Override
            public int compare(MeshyStreamFile streamFile1, MeshyStreamFile streamFile2) {
                return compareStreamFiles(streamFile1, streamFile2);
            }
        });
        return streamSources;
    }

    /** */
    int compareStreamFiles(final MeshyStreamFile streamFile1, final MeshyStreamFile streamFile2) {
        if (streamFile1.equals(streamFile2)) {
            return 0;
        }
        final String n1 = getSortOffset(streamFile1.name());
        final String n2 = getSortOffset(streamFile2.name());
        int c = n1.compareTo(n2);
        if (c == 0) {
            c = streamFile1.name().compareTo(streamFile2.name());
        }
        if (c == 0) {
            c = Integer.compare(streamFile1.hashCode(), streamFile2.hashCode());
        }
        return c;
    }

    @Override
    public void doShutdown() {
        if (!lateFileFindMap.isEmpty()) {
            log.warn("Late File Finds:\n{}", Joiner.on('\n').join(lateFileFindMap.entrySet()));
        }
        if (meshLink != null) {
            meshLink.close();
        } else {
            log.warn("Did not call meshLink.close() because meshLink is null.");
        }
        if (lastDate != null) {
            try {
                DateTime autoResumeDate = lastDate.minusDays(1);
                JSONObject jo = new JSONObject().put("lastDate", formatter.print(autoResumeDate)).put("moreData", moreData);
                Files.write(com.addthis.basis.util.Bytes.toBytes(jo.toString(2)), autoResumeFile);
            } catch (Exception ex) {
                log.warn("unable to write auto-resume file", ex);
            }
        }
    }
}
