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
package com.addthis.hydra.task.stream.mesh;

import java.io.IOException;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.codec.Codec;
import com.addthis.hydra.task.stream.AbstractPersistentStreamSource;
import com.addthis.hydra.task.stream.StreamFile;
import com.addthis.hydra.task.stream.StreamSourceByPaths;
import com.addthis.maljson.JSONObject;
import com.addthis.meshy.MeshyClient;

import com.google.common.io.Files;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class StreamSourceMeshPipe extends AbstractPersistentStreamSource {

    private static final Logger log = LoggerFactory.getLogger(StreamSourceMeshPipe.class);

    private static final String DEFAULT_MESH_HOST = Parameter.value("source.mesh.host", "localhost");
    private static final int DEFAULT_MESH_PORT = Parameter.intValue("source.mesh.port", 5000);
    private static final int DEFAULT_MESH_CACHE_UNITS = Parameter.intValue("source.mesh.cache_units", 1024 * 1024);

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

    private MeshyClient meshLink;
    private MeshHostScoreCache scoreCache;
    private DateTime firstDate;
    private DateTime lastDate;
    private StreamSourceByPaths<MeshyStreamFile> streamFileCache;
    private StreamSourceMeshSearch meshSearch;
    private Comparator<MeshyStreamFile> meshyFileComparator;



    @Override
    protected boolean doInit() throws IOException {
         /* establish link to the mesh */
        meshLink = new MeshyClient(meshHost, meshPort);
        log.info("mesh connected to " + meshHost + ":" + meshPort);
        scoreCache = new MeshHostScoreCache(meshLink);
        meshSearch = new StreamSourceMeshSearch(meshLink, meshPeerThreshold, meshShortCircuit, meshTimeOut,
                meshHost, meshShortCircuitWaitTime, pathOffSetFilter);
        streamFileCache = new StreamSourceByPaths();
        meshyFileComparator = new MeshyStreamFileComparator(sortOffSetFilter);
        return true;
    }

    @Override
    public StreamFile nextSource() {
        while (streamFileCache.isEmpty() && !dates.isEmpty()) {
            try {
                DateTime timeToLoad = dates.removeFirst();
                List<MeshyStreamFile> streamSources = getStreamSources(getDateTemplatedFileList(timeToLoad));
                streamFileCache.fillCache(formatter.print(timeToLoad), streamSources);
            } catch (Exception ex)  {
                log.warn("", ex);
                return null;
            }
        }
        MeshyStreamFile next = selectMeshHost(streamFileCache.nextGroup());
        return null;
    }

    void maybeUpdateDateMaxes (DateTime usedDateTime) {
        if (firstDate == null || usedDateTime.getMillis() < firstDate.getMillis()) {
            firstDate = usedDateTime;
            if (StreamSourceMeshPipe.log.isDebugEnabled()) {
                StreamSourceMeshPipe.log.debug("FIRST DATE = " + firstDate);
            }
        }
        if (lastDate == null || usedDateTime.getMillis() > lastDate.getMillis()) {
            lastDate = usedDateTime;
            if (StreamSourceMeshPipe.log.isDebugEnabled()) {
                StreamSourceMeshPipe.log.debug("LAST DATE = " + lastDate);
            }
        }
    }

    int getMeshBufferSize() {
        return meshStreamCache * meshStreamCacheUnits;
    }

    /**
     * Filters a list of MeshyStreamFiles, gets score values for each of them, and performs an inverse lottery
     * selection.
     */
    private MeshyStreamFile selectMeshHost(Collection<MeshyStreamFile> sourceCandidates) {
        if (sourceCandidates.size() == 1) {
            return sourceCandidates.iterator().next();
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
    private static <E extends StreamFile> List<E> filterFiles(Iterable<E> sourceCandidates) {
        long mostRecent = 0;
        long longest = 0;
        for (StreamFile file : sourceCandidates) {
            mostRecent = Math.max(mostRecent, file.lastModified());
            longest = Math.max(longest, file.length());
        }
        List<E> filteredSourceCandidates = new LinkedList<>();
        for (E file : sourceCandidates) {
            if ((file.lastModified() < mostRecent) || (file.length() < longest)) {
                log.warn("Ignoring a file choice from {} because it was behind.\n" +
                         "This is expected to happen rarely under normal conditions.", file);
            } else {
                filteredSourceCandidates.add(file);
            }
        }
        return filteredSourceCandidates;
    }

    /**
     * Performs an inverse lottery selection based on the scores provided. Higher scores mean you are less
     * likely to be selected. Each point in your score gives a ticket to everyone else.
     */
    private static MeshyStreamFile inverseLotterySelection(Map<MeshyStreamFile, Integer> scoreMap) {
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

    /** */
    private List<MeshyStreamFile> getStreamSources(String[] matches) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("getStreamSources :: " + Strings.join(matches, " -- "));
        }
        List<MeshyStreamFile> streamSources = meshSearch.findMeshFiles(matches);
        // sort files
        Collections.sort(streamSources, meshyFileComparator);
        if (reverse) {
            Collections.reverse(streamSources);
        }
        return streamSources;
    }


    @Override
    public void doShutdown() {
        if (!StreamSourceMeshSearch.lateFileFindMap.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Late File Finds:").append("\n");
            for (Map.Entry<String, Integer> entry : StreamSourceMeshSearch.lateFileFindMap.entrySet()) {
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
