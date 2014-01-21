package com.addthis.hydra.job.spawn;

import java.util.HashSet;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.hydra.data.util.JSONFetcher;
import com.addthis.hydra.task.stream.StreamFile;
import com.addthis.hydra.task.stream.StreamSourceMeshy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobAlertUtil {
    private final static Logger log =LoggerFactory.getLogger(JobAlertUtil.class);
    private final static String queryURLBase = "http://" + Parameter.value("spawn.queryhost") + ":2222/query/call";
    private final static String defaultOps = "gather=s";
    private final static long alertQueryTimeout = Parameter.longValue("alert.query.timeout", 20000l);
    private final static int alertQueryRetries = Parameter.intValue("alert.query.retries", 4);


    /**
     * Count the total byte sizes of files along a certain path via mesh
     * @param jobId The job to check
     * @param checkDate The date to check, generally in ymd format
     * @param dirPath The path to check within the jobId, e.g. split/{Y}{M}{D}/importantfiles/*.gz
     * @return A long representing the total size in bytes of files along the specified path
     */
    public static long getTotalBytesFromMesh(String jobId, String checkDate, String dirPath) {
        String meshLookupString = "/job*/" + jobId + "/*/gold/" + dirPath;
        // Use a StreamSourceMeshy to take advantage of its date expansion/replica deduplication
        StreamSourceMeshy mesh = new StreamSourceMeshy(new String[] {meshLookupString}, 1, checkDate, checkDate);
        StreamFile streamFile;
        long totalBytes = 0;
        while ((streamFile = mesh.nextSource()) != null) {
            totalBytes += streamFile.length();
        }
        return totalBytes;
    }

    /**
     * Count the total number of hits along a certain path in a tree object
     * @param jobId The job to query
     * @param checkPath The path to check, e.g.
     * @return The number of hits along the specified path
     */
    public static long getQueryCount(String jobId, String checkPath) {

        HashSet<String> result = JSONFetcher.staticLoadSet(getQueryURL(jobId, checkPath, defaultOps, defaultOps));
        if (result == null || result.isEmpty()) {
            log.warn("Found no data for job " + jobId + " checkPath=" + checkPath + "; returning zero");
            return 0;
        } else if (result.size() > 1) {
            log.warn("Found multiple results for job " + jobId + "checkPath=" + checkPath + "; using first row");
        }
        return Long.parseLong(result.iterator().next());

    }

    private static String getQueryURL(String jobId, String path, String ops, String rops) {
        return queryURLBase + "?job=" + jobId + "&path=" + expandDateMacro(path) + "&ops=" + Strings.urlEncode(ops) + "&rops=" + Strings.urlDecode(rops);
    }

    private static String expandDateMacro(String path) {
        // Needs to do {{now-1}} => 140103 (or whatever)
        return path;
    }
}
