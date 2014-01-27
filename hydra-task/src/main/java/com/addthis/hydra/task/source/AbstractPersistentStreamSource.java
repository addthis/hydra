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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.codec.Codec;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.task.stream.PersistentStreamFileSource;
import com.addthis.hydra.task.stream.StreamFileUtil;
import com.addthis.maljson.JSONObject;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract implementation of {@link com.addthis.hydra.task.stream.PersistentStreamFileSource}
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
    public static final String TIME_LAST_PROCESSED = "{{last}}";

    /**
     * The format of startDate and endDate values using the
     * <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html">DateTimeFormat</a>.
     * Default is either "source.mesh.date.format" configuration value or "YYMMdd".
     */
    @Codec.Set(codable = true)
    private String dateFormat = DEFAULT_DATE_FORMAT;


    /**
     * files that have been created before this date will not be processed. Default is {{last}}.
     */
    @Codec.Set(codable = true)
    private String startDate = TIME_LAST_PROCESSED;

    /**
     * files that have been created after this date will not be processed. Default is {{now}}.
     */
    @Codec.Set(codable = true)
    private String endDate = TIME_NOW;

    /**
     * If true then process the dates from the most recent date to the earliest date. Default is false.
     */
    @Codec.Set(codable = true)
    protected boolean reverse;

    /**
     * list of file paths to process. This field is required.
     */
    @Codec.Set(codable = true, required = true)
    private String[] files;

    /**
     * When selecting a substring of the input files for either sorting the file names
     * or fetching the file paths then use this token as the path separator.
     * Default is "source.mesh.path.token" configuration value or "/". *
     */
    @Codec.Set(codable = true)
    private String sortToken = DEFAULT_PATH_TOKEN;

    /**
     * shift the sorting suffix by this many characters. Default is 0.
     */
    @Codec.Set(codable = true)
    private int sortOffset;

    /**
     * skip this number of sortToken characters for the sorting suffix. Default is "source.mesh.sort.token.offset" configuration value or 5.
     */
    @Codec.Set(codable = true)
    private int sortTokenOffset = DEFAULT_SORT_TOKEN_OFFSET;

    /**
     * shift the generated file path by this many characters. Default is 0.
     */
    @Codec.Set(codable = true)
    private int pathOffset;

    /* skip this number of sortToken characters for generating file paths. Default is "source.mesh.path.token.offset" configuration value or 0. */
    @Codec.Set(codable = true)
    private int pathTokenOffset = DEFAULT_PATH_TOKEN_OFFSET;

    @Codec.Set(codable = true)
    private int jitterDays = 1;

    @Codec.Set(codable = true)
    private String startDateBaseDir;

    @Codec.Set(codable = true)
    private String dateIncrements;

    /* note: this is based on which files have been opened. If there is a large preOpen queue or many worker threads
     * then multiple days may be open at once, but this setting will assume that the latest day is the one to resume from. */
    @Codec.Set(codable = true)
    private boolean autoResume;

    protected final LinkedList<DateTime> dates = new LinkedList<>();
    protected DateTimeFormatter formatter;
    protected boolean moreData;
    private File stateDir;
    protected File autoResumeFile;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private static final List<String> TIME_CONSTANTS = new ArrayList<>(Arrays.asList("YY", "Y", "M", "D", "H"));

    /**
     * perform any initialization steps specific to the implementing class
     *
     * @return true if initialization was successful
     * @throws IOException
     */
    protected abstract boolean doInit() throws IOException;

    /**
     * perform any shutdown steps specific to the implementing class
     *
     * @throws IOException
     */
    public abstract void doShutdown() throws IOException;

    /**
     * Defines the directory where state for this source will be maintained
     *
     * @param dir
     */
    public void setStateDir(File dir) {
        stateDir = dir;
        autoResumeFile = new File(stateDir, "job.source");
    }

    /**
     * @return true if this source has more data to provide
     */
    public boolean hasMoreData() {
        return moreData;
    }

    /**
     * @return true if the configuration for this source includes a template 'mod' element
     *         that can be used to segment the input stream between n consumers
     */
    public boolean hasMod() {
        for (String file : files) {
            if (file.contains("{{mod")) {
                return true;
            }
        }
        return false;
    }

    /**
     * called by data source wrapper and performs common initialization
     * steps.
     */
    public boolean init(File stateDir, Integer[] shards) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("SSM: " + CodecJSON.encodeString(this));
        }
        setStateDir(stateDir);
        if (log.isTraceEnabled()) {
            log.trace("shards :: " + Strings.join(shards, " :: "));
        }
        /* expand files list */
        HashSet<String> matches = new HashSet<>();
        if (log.isTraceEnabled()) {
            log.trace("files.1 :: " + Strings.join(files, " -- "));
        }
        /* expand mods */
        for (String file : files) {
            for (Integer shard : shards) {
                matches.add(file.replace("{{mod}}", Strings.padleft(shard.toString(), 3, Strings.pad0)));
            }
        }
        files = matches.toArray(new String[matches.size()]);
        /* expand {US,DE,FR} bash-style string list */
        for (String file : files) {
            int io1 = file.indexOf("{");
            int io2 = file.indexOf("}");
            if (io1 >= 0 && io2 > io1) {
                String left = file.substring(0, io1);
                String right = file.substring(io2 + 1);
                for (String tok : Strings.splitArray(file.substring(io1 + 1, io2), ",")) {
                    // ignore reserved strings for time based expansion
                    if (!TIME_CONSTANTS.contains(tok)) {
                        String expand = left.concat(tok).concat(right);
                        matches.add(expand);
                        if (log.isTraceEnabled()) {
                            log.trace("expand " + file + " to " + expand);
                        }
                    }
                }
            }
        }
        files = matches.toArray(new String[matches.size()]);
        if (log.isTraceEnabled()) {
            log.trace("files.2 :: " + matches);
        }
        if (log.isTraceEnabled()) {
            log.trace("files.3 :: " + Strings.join(files, " -- "));
        }
        /* calculate start/end dates if required */
        formatter = DateTimeFormat.forPattern(dateFormat);
        if (autoResume && autoResumeFile.exists() && autoResumeFile.canRead() && autoResumeFile.length() > 0) {
            try {
                JSONObject jo = new JSONObject(Bytes.toString(Bytes.readFully(new FileInputStream(autoResumeFile))));
                String resumeDate = jo.optString("lastDate");
                if (resumeDate != null) {
                    log.warn("auto resume from " + jo);
                    startDate = resumeDate;
                }
            } catch (Exception ex) {
                log.warn("corrupted autoResume file: " + autoResumeFile + " ... " + ex);
            }
        }

        if (startDate == null) {
            log.warn("No startDate provided.");
            return false;
        }

        DateTime start = parseDateTime(startDate);
        if (endDate == null) {
            endDate = NOW_PREFIX + NOW_POSTFIX;
            log.warn("End Date not provided, using current time: " + endDate + " as end date for job");
        }
        DateTime end = parseDateTime(endDate);
        /* populate date list from start/end */
        fillDateList(start, end);
        log.info("[init] " + start + " to " + end + " = " + dates.size() + " time units");
        return doInit();
    }


    public void setStartTime(long time) {
        startDate = formatter.print(time);
        log.warn("override start date with " + startDate);
    }

    public void shutdown() throws IOException {
        running.set(false);
        doShutdown();
    }


    /**
     * @return a list of dates given the start/end range from the config
     */
    private void fillDateList(DateTime start, DateTime end) {
        DateTime mark = start;
        while (mark.isBefore(end) || mark.isEqual(end)) {
            if (reverse) {
                dates.addFirst(mark);
            } else {
                dates.addLast(mark);
            }
            if ((dateIncrements != null && dateIncrements.equals("DAYS")) || dateFormat.length() == 6) {
                mark = mark.plusDays(1);
            } else if ((dateIncrements != null && dateIncrements.equals("HOURS")) || dateFormat.length() == 8) {
                mark = mark.plusHours(1);
            } else if ((dateIncrements != null && dateIncrements.equals("MONTHS"))) {
                mark = mark.plusMonths(1);
            } else if (dateIncrements == null) {
                log.warn(
                        "Non-Standard dateFormat: " + dateFormat + " defaulting to daily time increments\n" +
                        "This can be modified to hourly time increments by setting dateIncrements to 'HOURS'");
                mark = mark.plusDays(1);
            }
        }
    }

    /** */
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

    /** */
    private int findDaysOffset(String time) {
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

    /** */
    private String replaceDateElements(DateTime time, String template) {
        template = template.replace("{YY}", time.year().getAsString());
        template = template.replace("{Y}", getTwoDigit(time.year().get()));
        template = template.replace("{M}", getTwoDigit(time.monthOfYear().get()));
        template = template.replace("{D}", getTwoDigit(time.dayOfMonth().get()));
        template = template.replace("{H}", getTwoDigit(time.hourOfDay().get()));
        if (log.isDebugEnabled()) {
            log.debug("template=" + template);
        }
        return template;
    }

    /** */
    private String getTwoDigit(int value) {
        if (value < 10) {
            return "0".concat(Integer.toString(value));
        }
        if (value > 99) {
            return getTwoDigit(value % 100);
        }
        return Integer.toString(value);
    }

    /** */
    public String[] getDateTemplatedFileList(DateTime timeToLoad) {
        String fileList[] = new String[files.length];
        for (int i = 0; i < files.length; i++) {
            fileList[i] = replaceDateElements(timeToLoad, files[i]);
        }
        return fileList;
    }

    /**
     * return substring getSortOffset into file name
     */
    public String getSortOffset(String name) {
        int sortOff = sortOffset;
        if (sortToken != null && sortTokenOffset > 0) {
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

    /**
     * return substring getSortOffset into file name
     */
    public String getPathOffset(String name) {
        return StreamFileUtil.getCanonicalFileReferenceCacheKey(name, pathOffset, sortToken, pathTokenOffset);
    }

    public String[] getFiles() {
        return files;
    }

    public void setFiles(String[] files) {
        this.files = files;
    }

    public void setSortTokenOffset(int sortTokenOffset) {
        this.sortTokenOffset = sortTokenOffset;
    }

    public void setPathTokenOffset(int pathTokenOffset) {
        this.pathTokenOffset = pathTokenOffset;
    }
}
