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
package com.addthis.hydra.job.spawn;

import java.io.IOException;

import java.util.Collection;
import java.util.HashSet;
import java.util.regex.Pattern;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.data.filter.bundle.BundleFilter;
import com.addthis.hydra.data.util.DateUtil;
import com.addthis.hydra.data.util.JSONFetcher;
import com.addthis.hydra.task.stream.StreamFileUtil;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.StringUtils;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobAlertUtil {
    private static final Logger log = LoggerFactory.getLogger(JobAlertUtil.class);
    private static final String queryURLBase = "http://" + Parameter.value("spawn.queryhost") + ":2222/query/call";
    private static final String defaultOps = "gather=s";
    private static final int alertQueryTimeout = Parameter.intValue("alert.query.timeout", 20_000);
    private static final int alertQueryRetries = Parameter.intValue("alert.query.retries", 4);
    @VisibleForTesting
    static final DateTimeFormatter ymdFormatter = new DateTimeFormatterBuilder().appendTwoDigitYear(2000).appendMonthOfYear(2).appendDayOfMonth(2).toFormatter();
    private static final int pathTokenOffset = Parameter.intValue("source.mesh.path.token.offset", 2);
    private static final int pathOff = Parameter.intValue("source.mesh.path.offset", 0);
    private static final String sortToken = Parameter.value("source.mesh.path.token", "/");
    private static final Pattern QUERY_TRIM_PATTERN = Pattern.compile("[\\[\\]]");

    /**
     * Count the total byte sizes of files along a certain path via mesh
     * @param jobId The job to check
     * @param dirPath The path to check within the jobId, e.g. split/{{now-1}}/importantfiles/*.gz
     * @return A long representing the total size in bytes of files along the specified path
     */
    public static long getTotalBytesFromMesh(MeshyClient meshyClient, String jobId, String dirPath) {
        String meshLookupString = "/job*/" + jobId + "/*/gold/" + expandDateMacro(dirPath);
        if (meshyClient != null) {
            try {
                Collection<FileReference> fileRefs = meshyClient.listFiles(new String[]{meshLookupString});
                HashSet<String> fileRefKeysUsed = new HashSet<>();
                long totalBytes = 0;
                for (FileReference fileRef : fileRefs) {
                    // Use StreamSourceMeshy to generate a canonical path key. In particular, strip off any multi-minion prefixes if appropriate.
                    String meshFileKey = StreamFileUtil.getCanonicalFileReferenceCacheKey(fileRef.name, pathOff, sortToken, pathTokenOffset);
                    if (!fileRefKeysUsed.contains(meshFileKey)) {
                        totalBytes += fileRef.size;
                        fileRefKeysUsed.add(meshFileKey);
                    }
                }
                return totalBytes;
            } catch (IOException e) {
                log.warn("Job alert mesh look up failed", e);
            }
        }
        else {
            log.warn("Received mesh lookup request job={} dirPath={} while meshy client was not instantiated; returning zero", jobId, dirPath);
        }
        return 0;
    }

    /**
     * Count the total number of hits along a certain path in a tree object
     * @param jobId The job to query
     * @param checkPath The path to check, e.g.
     * @return The number of hits along the specified path
     */
    public static long getQueryCount(String jobId, String checkPath) {

        HashSet<String> result = JSONFetcher.staticLoadSet(getQueryURL(jobId, checkPath, defaultOps, defaultOps),
                alertQueryTimeout, alertQueryRetries, null);
        if (result == null || result.isEmpty()) {
            log.warn("Found no data for job={} checkPath={}; returning zero", jobId, checkPath);
            return 0;
        } else if (result.size() > 1) {
            log.warn("Found multiple results for job={} checkPath={}; using first row", jobId, checkPath);
        }
        String raw = result.iterator().next();
        return Long.parseLong(QUERY_TRIM_PATTERN.matcher(raw).replaceAll("")); // Trim [] characters and parse as long

    }

    private static boolean[] testQueryResult(JSONArray array, BundleFilter filter) {
        boolean[] result = new boolean[array.length() - 1];
        JSONArray headerRow = array.optJSONArray(0);
        String[] header = new String[headerRow.length()];
        for(int i = 0; i < header.length; i++) {
            header[i] = headerRow.optString(i);
        }
        for(int i = 1; i < array.length(); i++) {
            JSONArray row = array.optJSONArray(i);
            Bundle bundle = new ListBundle();
            BundleFormat format = bundle.getFormat();
            for(int j = 0; j < row.length(); j++) {
                bundle.setValue(format.getField(header[j]), ValueFactory.create(row.optString(j)));
            }
            try {
                result[i - 1] = filter.filter(bundle);
                log.trace("Row {} filter result is {}", i - 1, result[i-1]);
            } catch(Exception ex) {
                log.warn("Error while evaluating row {}: {}", i - 1, ex);
                result[i - 1] = false;
            }
        }
        return result;
    }

    public static boolean[] evaluateQueryWithFilter(JobAlert alert, String jobId) {
        String query = alert.getCanaryPath();
        String ops = alert.getCanaryOps();
        String rops = alert.getCanaryRops();
        String filter = alert.getCanaryFilter();
        String url = getQueryURL(jobId, query, ops, rops);
        log.trace("Emitting query with url {}", url);
        JSONArray array = JSONFetcher.staticLoadJSONArray(url, alertQueryTimeout, alertQueryRetries);
        alert.appendCanaryOutputMessage(array.toString() + "\n");
        boolean valid = true;
        /**
         * Test the following conditions:
         * - the array contains two or more values
         * - each value of the array is itself an array
         * - the lengths of all subarrays are identical
         */
        valid = valid && array.length() > 1;
        log.trace("Array contains two or more values: {}", array.length() > 1);
        JSONArray header = valid ? array.optJSONArray(0) : null;
        valid = valid && (header != null);
        log.trace("Header is an array: {}", header != null);
        for(int i = 1; valid && i < array.length(); i++) {
            JSONArray element = array.optJSONArray(i);
            log.trace("Element {} is an array: {}", i, element != null);
            if (element != null) {
                valid = valid && (element.length() == header.length());
                log.trace("Element {} has correct length: {}", i, element.length() == header.length());
            } else {
                valid = false;
            }
        }
        BundleFilter bFilter = null;
        try {
            bFilter = CodecJSON.decodeObject(BundleFilter.class, new JSONObject(filter));
        } catch (Exception ex) {
            alert.appendCanaryOutputMessage("Error attempting to create bundle filter: " + ex + "\n");
            log.error("Error attempting to create bundle filter {}", ex);
            valid = false;
        }
        if (valid) {
            return testQueryResult(array, bFilter);
        } else {
            boolean[] result = new boolean[1];
            result[0] = false;
            return result;
        }
    }

    private static String getQueryURL(String jobId, String path, String ops, String rops) {
        return queryURLBase + "?job=" + jobId + "&path=" + Strings.urlEncode(expandDateMacro(path))
               + "&ops=" + Strings.urlEncode(ops) + "&rops=" + Strings.urlEncode(rops);
    }

    /**
     * Split a path up and replace any {{now-1}}-style elements with the YYMMDD equivalent
     * @param path The input path to process
     * @return The path with the relevant tokens replaced
     */
    @VisibleForTesting
    static String expandDateMacro(String path) {
        String[] parts = StringUtils.splitByWholeSeparatorPreserveAllTokens(path, DateUtil.NOW_PREFIX);
        StringBuilder sb = new StringBuilder(parts[0]);
        for (int i = 1; i<parts.length; i++) {
            String part = parts[i];
            int end = part.indexOf(DateUtil.NOW_POSTFIX);
            if (end > -1) {
                String dateEnd = part.substring(0, end + 2);
                sb.append(DateUtil.getDateTime(ymdFormatter, DateUtil.NOW_PREFIX + dateEnd).toString(ymdFormatter));
                sb.append(part.substring(end + 2));
            } else {
                sb.append(DateUtil.NOW_PREFIX);
                sb.append(part);
            }
        }
        return sb.toString();
    }
}
