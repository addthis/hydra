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
package com.addthis.hydra.task.source;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.task.stream.PersistentStreamFileSource;
import com.addthis.hydra.task.stream.StreamFileUtil;
import com.addthis.maljson.JSONObject;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract implementation of {@link PersistentStreamFileSource}
 * that provides much of the base functionality required to implement a streaming file source.
 * The main purpose of this class is to parse an input configuration in order to
 * to provide common necessary inputs that concrete implementations require to
 * identify the files from the data source should provide to clients.
 */
public abstract class AbstractPersistentStreamSource implements PersistentStreamFileSource {

    private static final Logger log = LoggerFactory.getLogger(AbstractPersistentStreamSource.class);

    // note that for historical reason these parameters use 'mesh' in their descriptions
    private static final String DEFAULT_DATE_FORMAT = Parameter.value("source.mesh.date.format", "YYMMdd");
    private static final int DEFAULT_SORT_TOKEN_OFFSET = Parameter.intValue("source.mesh.sort.token.offset", 5);
    private static final int DEFAULT_PATH_TOKEN_OFFSET = Parameter.intValue("source.mesh.path.token.offset", 0);
    private static final String DEFAULT_PATH_TOKEN = Parameter.value("source.mesh.path.token", "/");

    public static final long ONE_HOUR_IN_MILLIS = 60 * 60 * 1000;
    public static final long ONE_DAY_IN_MILLIS = 24 * ONE_HOUR_IN_MILLIS;

    private static final String NOW_PREFIX = "{{now";
    private static final String NOW_POSTFIX = "}}";
    public static final String TIME_NOW = "{{now}}";
    private static final Pattern MOD_PATTERN = Pattern.compile("{{mod}}", Pattern.LITERAL);
    private static final Pattern YY_PATTERN = Pattern.compile("{YY}", Pattern.LITERAL);
    private static final Pattern Y_PATTERN = Pattern.compile("{Y}", Pattern.LITERAL);
    private static final Pattern M_PATTERN = Pattern.compile("{M}", Pattern.LITERAL);
    private static final Pattern D_PATTERN = Pattern.compile("{D}", Pattern.LITERAL);
    private static final Pattern H_PATTERN = Pattern.compile("{H}", Pattern.LITERAL);

    /**
     * The format of startDate and endDate values using the
     * <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html">DateTimeFormat</a>.
     * Default is either "source.mesh.date.format" configuration value or "YYMMdd".
     */
    @JsonProperty private String dateFormat = DEFAULT_DATE_FORMAT;

    /** files that have been created before this date will not be processed. Default is {{last}}. */
    @JsonProperty private String startDate = TIME_NOW;

    /** files that have been created after this date will not be processed. Default is {{now}}. */
    @JsonProperty private String endDate = TIME_NOW;

    /** If true then process the dates from the most recent date to the earliest date. Default is false. */
    @JsonProperty protected boolean reverse;

    /** list of file paths to process. This field is required. */
    @JsonProperty(required = true) private List<String> files;

    /**
     * When selecting a substring of the input files for either sorting the file names
     * or fetching the file paths then use this token as the path separator.
     * Default is "source.mesh.path.token" configuration value or "/". *
     */
    @JsonProperty private String sortToken = DEFAULT_PATH_TOKEN;

    /** shift the sorting suffix by this many characters. Default is 0. */
    @JsonProperty private int sortOffset;

    /**
     * skip this number of sortToken characters for the sorting suffix.
     * Default is "source.mesh.sort.token.offset" configuration value or 5.
     */
    @JsonProperty private int sortTokenOffset = DEFAULT_SORT_TOKEN_OFFSET;

    /** shift the generated file path by this many characters. Default is 0. */
    @JsonProperty private int pathOffset;

    /**
     * skip this number of sortToken characters for generating file paths.
     * Default is "source.mesh.path.token.offset" configuration value or 0.
     */
    @JsonProperty private int pathTokenOffset = DEFAULT_PATH_TOKEN_OFFSET;

    @JsonProperty private int jitterDays = 1;

    @JsonProperty private String startDateBaseDir;

    @JsonProperty private String dateIncrements;

    /* note: this is based on which files have been opened. If there is a large preOpen queue or many worker threads
     * then multiple days may be open at once, but this setting will assume that the latest day is the one to resume from. */
    @JsonProperty private boolean autoResume;

    protected final LinkedList<DateTime> dates = new LinkedList<>();
    protected DateTimeFormatter formatter;
    protected volatile boolean moreData;
    private File stateDir;
    protected File autoResumeFile;
    private final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * perform any initialization steps specific to the implementing class
     *
     * @return true if initialization was successful
     */
    protected abstract boolean doInit() throws IOException;

    /** perform any shutdown steps specific to the implementing class */
    public abstract void doShutdown() throws IOException;

    /**
     * @return true if the configuration for this source includes a template 'mod' element
     *         that can be used to segment the input stream between n consumers
     */
    @Override public boolean hasMod() {
        for (String file : files) {
            if (file.contains("{{mod")) {
                return true;
            }
        }
        return false;
    }

    /** called by data source wrapper and performs common initialization steps. */
    @Override public boolean init(File stateDir, Integer[] shards) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("SSM: {}", CodecJSON.encodeString(this));
        }
        this.stateDir = stateDir;
        autoResumeFile = new File(this.stateDir, "job.source");
        if (log.isTraceEnabled()) {
            log.trace("shards :: {}", Strings.join(shards, " :: "));
        }
        /* expand files list */
        Set<String> matches = new HashSet<>();
        log.trace("files.1 :: {}", files);
        /* expand mods */
        for (String file : files) {
            for (Integer shard : shards) {
                matches.add(MOD_PATTERN.matcher(file).replaceAll(Strings.padleft(shard.toString(), 3, Strings.pad0)));
            }
        }
        matches = expandPaths(matches);
        log.trace("files.2 :: {}", matches);
        files = new ArrayList<>(matches);
        log.trace("files.3 :: {}", files);
        /* calculate start/end dates if required */
        formatter = DateTimeFormat.forPattern(dateFormat);
        if (autoResume && autoResumeFile.exists() && autoResumeFile.canRead() && autoResumeFile.length() > 0) {
            try {
                JSONObject jo = new JSONObject(Bytes.toString(Bytes.readFully(new FileInputStream(autoResumeFile))));
                String resumeDate = jo.optString("lastDate");
                if (resumeDate != null) {
                    log.warn("auto resume from {}", jo);
                    startDate = resumeDate;
                }
            } catch (Exception ex) {
                log.warn("corrupted autoResume file: {}", autoResumeFile, ex);
            }
        }

        if (startDate == null) {
            log.warn("No startDate provided.");
            return false;
        }

        DateTime start = parseDateTime(startDate);
        if (endDate == null) {
            endDate = NOW_PREFIX + NOW_POSTFIX;
            log.warn("End Date not provided, using current time: {} as end date for job", endDate);
        }
        DateTime end = parseDateTime(endDate);
        /* populate date list from start/end */
        fillDateList(start, end);
        log.info("[init] {} to {} = {} time units", start, end, dates.size());
        return doInit();
    }

    protected Set<String> expandPaths(Set<String> paths) {
        return paths;
    }

    public void setStartTime(long time) {
        startDate = formatter.print(time);
        log.warn("override start date with {}", startDate);
    }

    @Override public void shutdown() throws IOException {
        running.set(false);
        doShutdown();
    }

    /** list of dates given the start/end range from the config */
    private void fillDateList(DateTime start, DateTime end) {
        DateTime mark = start;
        while (mark.isBefore(end) || mark.isEqual(end)) {
            if (reverse) {
                dates.addFirst(mark);
            } else {
                dates.addLast(mark);
            }
            if ("DAYS".equals(dateIncrements) || (dateFormat.length() == 6)) {
                mark = mark.plusDays(1);
            } else if ("HOURS".equals(dateIncrements) || (dateFormat.length() == 8)) {
                mark = mark.plusHours(1);
            } else if ("MONTHS".equals(dateIncrements)) {
                mark = mark.plusMonths(1);
            } else if (dateIncrements == null) {
                log.warn("Non-Standard dateFormat: {} defaulting to daily time increments\nThis can be modified to " +
                         "hourly time increments by setting dateIncrements to 'HOURS'", dateFormat);
                mark = mark.plusDays(1);
            }
        }
    }

    private DateTime parseDateTime(String dateString) {
        DateTime time;
        if (dateString.contains(NOW_PREFIX)) {
            // TODO: be better to get this time from a service
            time = new DateTime();
            time = time.plusDays(findDaysOffset(dateString));
        } else {
            time = formatter.parseDateTime(dateString);
        }
        return time;
    }

    private static int findDaysOffset(String time) {
        int startIndex = time.indexOf(NOW_PREFIX) + 6;
        int endIndex = time.indexOf(NOW_POSTFIX);
        if (startIndex < 0 || endIndex <= startIndex) {
            return 0;
        }
        int offset = Integer.parseInt(time.substring(startIndex, endIndex));
        if (time.charAt(startIndex - 1) == '-') {
            offset = 0 - offset;
        }
        return offset;
    }

    private static String replaceDateElements(DateTime time, String template) {
        String result = YY_PATTERN.matcher(template).replaceAll(time.year().getAsString());
        result = Y_PATTERN.matcher(template).replaceAll(getTwoDigit(time.year().get()));
        result = M_PATTERN.matcher(template).replaceAll(getTwoDigit(time.monthOfYear().get()));
        result = D_PATTERN.matcher(template).replaceAll(getTwoDigit(time.dayOfMonth().get()));
        result = H_PATTERN.matcher(template).replaceAll(getTwoDigit(time.hourOfDay().get()));
        log.debug("template={}, result={}", template, result);
        return result;
    }

    private static String getTwoDigit(int value) {
        if (value < 10) {
            return "0".concat(Integer.toString(value));
        }
        if (value > 99) {
            return getTwoDigit(value % 100);
        }
        return Integer.toString(value);
    }

    public String[] getDateTemplatedFileList(final DateTime timeToLoad) {
        List<String> fileList = Lists.transform(files, new Function<String, String>() {
            @Override public String apply(String input) {
                return replaceDateElements(timeToLoad, input);
            }
        });
        return fileList.toArray(new String[fileList.size()]);
    }

    /** return substring getSortOffset into file name */
    public String getSortOffset(String name) {
        int sortOff = sortOffset;
        if ((sortToken != null) && (sortTokenOffset > 0)) {
            int pos = 0;
            int off = sortTokenOffset;
            while (off-- > 0 && (pos = name.indexOf(sortToken, pos)) >= 0) {
                pos++;
            }
            if (pos > 0) {
                sortOff += pos;
            }
        }
        return name.substring(sortOff);
    }

    /** return substring getSortOffset into file name */
    public String getPathOffset(String name) {
        return StreamFileUtil.getCanonicalFileReferenceCacheKey(name, pathOffset, sortToken, pathTokenOffset);
    }
}
