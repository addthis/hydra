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
import java.io.InputStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.io.IOWrap;
import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.codec.Codec;
import com.addthis.hydra.task.source.AbstractPersistentStreamSource;
import com.addthis.maljson.JSONObject;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.file.FileSource;
import com.addthis.meshy.service.stream.StreamSource;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import com.ning.compress.lzf.LZFInputStream;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

import static com.google.common.base.Objects.toStringHelper;
import lzma.sdk.lzma.Decoder;
import lzma.streams.LzmaInputStream;

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

    private static final String DEFAULT_MESH_HOST = Parameter.value("source.mesh.host", "localhost");
    private static final int DEFAULT_MESH_PORT = Parameter.intValue("source.mesh.port", 5000);
    private static final int DEFAULT_MESH_CACHE_UNITS = Parameter.intValue("source.mesh.cache_units", 1024 * 1024);
    private static final Splitter HOST_METADATA_SPLITTER = Splitter.on(',').omitEmptyStrings();

    private static final int DEFAULT_MESH_TIMEOUT = Parameter.intValue("source.mesh.find.timeout", 2000);
    private static final boolean DEFAULT_MESH_SHORT_CIRCUIT = Parameter.boolValue("source.mesh.find.short.circuit", true);

    /**
     * the percentage of peers that must respond before the finder returns
     */
    @Codec.Set(codable = true)
    private double meshPeerThreshold = 0.95;

    /**
     * if true the finder may return before 100% of peers have responded
     */
    @Codec.Set(codable = true)
    private boolean meshShortCircuit = DEFAULT_MESH_SHORT_CIRCUIT;

    /**
     * the amount of time the mesh finder will wait before potentially returning
     */
    @Codec.Set(codable = true)
    private int meshTimeOut = DEFAULT_MESH_TIMEOUT;

    /**
     * Hostname of the meshy server. Default is either "source.mesh.host" configuration value or "localhost".
     */
    @Codec.Set(codable = true)
    private String meshHost = DEFAULT_MESH_HOST;

    /**
     * Port number of the meshy server. Default is either "source.mesh.port" configuration value or 5000.
     */
    @Codec.Set(codable = true)
    private int meshPort = DEFAULT_MESH_PORT;

    /**
     * Default mesh node cache per stream in size of (meshStreamCacheUnits * bytes).
     */
    @Codec.Set(codable = true)
    private int meshStreamCache;

    @Codec.Set(codable = true)
    private int maxRangeDays;

    @Codec.Set(codable = true)
    private int maxRangeHours;

    /**
     * Number of byte units for meshStreamCache parameter.
     * Default is either "source.mesh.cache_units" configuration value or 1 MB (2<sup>20</sup> bytes)
     */
    @Codec.Set(codable = true)
    private int meshStreamCacheUnits = DEFAULT_MESH_CACHE_UNITS;

    /**
     * Length of time to wait before possibly short circuiting mesh lookups
     */
    @Codec.Set(codable = true)
    private int meshShortCircuitWaitTime = 5000;


    private final LinkedList<String> cache = new LinkedList<>();
    private static final ConcurrentHashMap<String, Integer> lateFileFindMap = new ConcurrentHashMap<>();
    private final HashMap<String, List<MeshyStreamFile>> cacheMap = new HashMap<>();

    private MeshyClient meshLink;
    private MeshHostScoreCache scoreCache;
    private DateTime firstDate;
    private DateTime lastDate;

    private volatile int peerCount = -1;

    @Override
    protected boolean doInit() throws IOException {
         /* establish link to the mesh */
        meshLink = new MeshyClient(meshHost, meshPort);
        log.info("mesh connected to " + meshHost + ":" + meshPort);
        scoreCache = new MeshHostScoreCache(meshLink);
        return true;
    }

    /**
     * query the mesh for a list of matching files
     */
    private List<MeshyStreamFile> findMeshFiles(final DateTime date, String[] patterns) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("find using mesh=" + meshLink + " patterns=" + patterns);
        }
        // responding peer count should only be mutated by meshy threads
        final AtomicInteger respondingPeerCount = new AtomicInteger();
        final Semaphore gate = new Semaphore(1);
        final ConcurrentHashMap<String, Histogram> responseTimeMap = new ConcurrentHashMap<>();

        final List<MeshyStreamFile> fileReferences = new ArrayList<>();
        try {
            gate.acquire();
        } catch (InterruptedException e) {
            throw new IOException("interrupted while waiting for gate");
        }
        final long startTime = System.currentTimeMillis();
        final AtomicBoolean shortCircuited = new AtomicBoolean(false);
        peerCount = -1;
        FileSource source = new FileSource(meshLink, patterns, "localF") {
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
                                    Sets.newHashSet(HOST_METADATA_SPLITTER.split(ref.getHostUUID()));
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
                            respondingPeerCount.set(Math.max(newCompleteResponsesCount, respondingPeerCount.get()));
                            StreamSourceMeshy.log.debug(toLogString("response"));
                            return;
                        case "localfind":
                            localMeshFindRunning = false;
                            unfinishedHosts.remove(meshHost);
                            respondingPeerCount.incrementAndGet();
                            StreamSourceMeshy.log.debug(toLogString("localfind"));
                            return;
                        default:
                            StreamSourceMeshy.log.warn("Found a file ref without a prepended /. Assuming its a real fileref for now : {}", ref.name);
                    }
                }
                String hostId = ref.getHostUUID().substring(0, ref.getHostUUID().indexOf("-"));
                if (shortCircuited.get()) {
                    int lateCount = (lateFileFindMap.get(hostId) == null ? 1 : lateFileFindMap.get(hostId));
                    lateFileFindMap.put(hostId, lateCount + 1);
                    // we are done here
                    return;
                }
                long receiveTime = System.currentTimeMillis();
                fileReferences.add(new MeshyStreamFile(date, ref));
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
        while (true) {
            try {
                if (!gate.tryAcquire(meshShortCircuitWaitTime, TimeUnit.MILLISECONDS)) {
                    if (meshShortCircuit
                        && peerCount > 0
                        && (System.currentTimeMillis() - startTime) > meshTimeOut
                        && respondingPeerCount.get() > (meshPeerThreshold * peerCount)) {
                        // break early
                        shortCircuited.set(true);
                        log.warn("Breaking after receiving responses from " + respondingPeerCount.get() + " of " + peerCount + " peers");
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
        while (cache.size() == 0 && dates.size() > 0) {
            try {
                fillCache(dates.removeFirst());
            } catch (Exception ex)  {
                log.warn("", ex);
                return null;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("@ next source dates=" + dates.size() + " cache=" + cache.size() + " peek=" + cache.peekFirst() + " map=" + cacheMap.get(cache.peekFirst()));
        }
        if (cache.size() > 0) {
            String cacheKey;
            if (reverse) {
                cacheKey = cache.removeLast();
            } else {
                cacheKey = cache.removeFirst();
            }
            MeshyStreamFile next = selectMeshHost(cacheMap.remove(cacheKey));
            if (maxRangeDays > 0 && firstDate != null) {
                if (next.date.getMillis() - firstDate.getMillis() > ONE_DAY_IN_MILLIS * maxRangeDays) {
                    log.warn("truncating source list. over max days: " + maxRangeDays);
                    moreData = true;
                    return null;
                }
            }
            if (maxRangeHours > 0 && firstDate != null) {
                if (next.date.getMillis() - firstDate.getMillis() > ONE_HOUR_IN_MILLIS * maxRangeHours) {
                    log.warn("truncating source list. over max hours: " + maxRangeHours);
                    moreData = true;
                    return null;
                }
            }
            return next;
        }
        return null;
    }

    /**
     * Filters a list of MeshyStreamFiles, gets score values for each of them, and performs an inverse lottery
     * selection.
     */
    private MeshyStreamFile selectMeshHost(List<MeshyStreamFile> sourceCandidates) {
        if (sourceCandidates.size() == 1) {
            return sourceCandidates.get(0);
        }
        sourceCandidates = filterFiles(sourceCandidates);
        Map<MeshyStreamFile, Integer> scoreMap = new HashMap<>();
        for (MeshyStreamFile file : sourceCandidates) {
            int score = scoreCache.get(file.meshFile.getHostUUID());
            scoreMap.put(file, score);
        }
        return inverseLotterySelection(scoreMap);
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
        List<MeshyStreamFile> filteredSourceCandidates = new LinkedList<>();
        for (MeshyStreamFile file : sourceCandidates) {
            if (file.date.isBefore(mostRecent) || file.length() < longest) {
                log.warn("Ignoring a file choice from " + file.meshFile.getHostUUID() + " because it was behind." +
                         "\nThis is expected to happen rarely under normal conditions.");
                continue;
            }
            filteredSourceCandidates.add(file);
        }
        return filteredSourceCandidates;
    }

    /**
     * Performs an inverse lottery selection based on the scores provided. Higher scores mean you are less
     * likely to be selected. Each point in your score gives a ticket to everyone else.
     */
    private MeshyStreamFile inverseLotterySelection(Map<MeshyStreamFile, Integer> scoreMap) {
        int totalScore = 0;
        for (Integer i : scoreMap.values()) {
            totalScore += i;
        }
        int adjustedTotalScore = totalScore * (scoreMap.size() - 1);
        int selection = (int) (adjustedTotalScore * Math.random());
        for (Map.Entry<MeshyStreamFile, Integer> e : scoreMap.entrySet()) {
            int adjustedScore = totalScore - e.getValue();
            adjustedTotalScore -= adjustedScore;
            if (selection >= adjustedTotalScore) {
                return e.getKey();
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("There was an error in lottery selection; defaulting to the first choice.");
        }
        return scoreMap.keySet().iterator().next();
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
                sourceOptions = new LinkedList<>();
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

    /** */
    class MeshyStreamFile implements StreamFile {

        private final FileReference meshFile;
        private final DateTime date;

        MeshyStreamFile(DateTime date, FileReference meshFile) {
            this.date = date;
            this.meshFile = meshFile;
        }

        @Override
        public String toString() {
            return "{n=" + name() + ",p=" + getPath() + ",u=" + meshFile.getHostUUID() + "}";
        }

        @Override
        public InputStream getInputStream() throws IOException {
            if (firstDate == null || date.getMillis() < firstDate.getMillis()) {
                firstDate = date;
                if (log.isDebugEnabled()) {
                    log.debug("FIRST DATE = " + firstDate);
                }
            }
            if (lastDate == null || date.getMillis() > lastDate.getMillis()) {
                lastDate = date;
                if (log.isDebugEnabled()) {
                    log.debug("LAST DATE = " + lastDate);
                }
            }
            // this fails on linux with out the explicit cast to InputStream
            InputStream in = new StreamSource(meshLink, meshFile.getHostUUID(), meshFile.name, meshStreamCache * meshStreamCacheUnits).getInputStream();
            if (name().endsWith(".gz")) {
                in = IOWrap.gz(in, 4096);
            } else if (name().endsWith(".lzf")) {
                in = new LZFInputStream(in);
            } else if (name().endsWith(".snappy")) {
                in = new SnappyInputStream(in);
            } else if (name().endsWith(".bz2")) {
                in = new BZip2CompressorInputStream(in, true);
            } else if (name().endsWith(".lzma")) {
                in = new LzmaInputStream(in, new Decoder());
            }
            return in;
        }

        @Override
        public long lastModified() {
            return meshFile.lastModified;
        }

        @Override
        public long length() {
            return meshFile.size;
        }

        @Override
        public String name() {
            return meshFile.name;
        }

        @Override
        public String getPath() {
            return getPathOffset(meshFile.name);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof MeshyStreamFile)) {
                return false;
            }
            MeshyStreamFile otherFile = (MeshyStreamFile) other;
            if (!Objects.equals(meshFile, otherFile.meshFile)) {
                return false;
            }
            if (!Objects.equals(date, otherFile.date)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(meshFile, date);
        }
    }

    @Override
    public void doShutdown() {
        if (lateFileFindMap.size() > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append("Late File Finds:").append("\n");
            for (Map.Entry<String, Integer> entry : lateFileFindMap.entrySet()) {
                sb.append("\t").append(entry.getKey()).append(" - ").append(entry.getValue()).append("\n");
            }
            log.warn(sb.toString());
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
                Files.write(Bytes.toBytes(jo.toString(2)), autoResumeFile);
            } catch (Exception ex) {
                log.warn("unable to write auto-resume file: " + ex);
            }
        }
    }
}
