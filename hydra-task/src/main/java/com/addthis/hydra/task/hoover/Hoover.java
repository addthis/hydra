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
package com.addthis.hydra.task.hoover;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;

import com.addthis.basis.util.Backoff;
import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.Strings;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.common.hash.PluggableHashFunction;
import com.addthis.hydra.data.util.DateUtil;
import com.addthis.hydra.task.run.TaskRunConfig;
import com.addthis.hydra.task.run.TaskRunnable;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * This Hydra job is <span class="hydra-summary">a bulk file loader for Hydra clusters</span>.
 * <p/>
 * <p>The hoover job queries the servers specified by the
 * {@link #hosts} parameter. The file contents of each remote server is queried at the
 * of the file path specified by {@link #path} parameter.
 * Each file is tested to determine if it should be transferred onto the local server.
 * The files that meet all the criteria are transferred to the local server into
 * the path specified by the {@link #outDir} and {@link #pathOut} parameters.</p>
 * Any files on the local server in output directory that are older
 * than {@link #purgeAfterDays} days will be deleted. The file purge will use the
 * UNIX file modification time to determine which files are purged. The file modification
 * time may be different from the date and time that is extracted from the file name or path.
 * <p/>
 * <p>When a file is detected on a remove server then the date of the file is extracted from
 * the file name or the file path. If the date lies outside of a specified date and time interval,
 * then the file is not transferred. The date is extracted using either the file name (by default) or
 * the file path (if the {@link #pathBasedDateMatching pathBasedDateMatching} parameter is true).
 * The regular expression specified in the {@link #dateMatcher dateMatcher} parameter is used to retrieve
 * the date from the file name or path. The date string is interpreted as a date and time using the
 * DateTimeFormat format specified in the {@link #dateExtractor dateExtractor} parameter.
 * <p>Each file is tested to determine whether it should be transferred from the remote server.
 * The following criteria must be met:
 * <ul>
 * <li>the file name matches the {@link #match} regular expression (if 'match' is non-null),
 * <li>the file lies within the start and end date time interval,
 * <li>the hash of the file name is within the shard list,
 * <li>the file was not already fetched in a previous run.
 * </ul>
 * <p>Example:</p>
 * <pre>{
 *   type : "hoover",
 *   user : "app",
 *   hosts : {
 *     "app1" : "web1.local",
 *     "app2" : "app2.local",
 *   },
 *   path : "/home/app/dat/out/history/*.gz",
 *   pathOut : ["{{YY}}","{{M}}","{{D}}","{{HOST}}-{{FILE}}"],
 *   dateMatcher : "([0-9]+)-[0-9]+.cnk.gz",
 *   dateExtractor : "yyMMdd",
 *   purgeAfterDays : 450,
 * }</pre>
 *
 * @user-reference
 * @hydra-name hoover
 */
public class Hoover extends TaskRunnable implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Hoover.class);
    private static final SimpleDateFormat dateOut = new SimpleDateFormat("yyyyMMddHH");

    /**
     * Mapping from server aliases to server hostnames.
     * The serves alias will be substituted in place of the variable {{HOST}}.
     */
    @FieldConfig(codable = true)
    private HashMap<String, String> hosts;

    /**
     * Mark file directory on the local machine. Default is "hoover.mark".
     */
    @FieldConfig(codable = true)
    private String markDir = "hoover.mark";

    /**
     * Output directory on the local machine. Default is "hoover.out".
     */
    @FieldConfig(codable = true)
    private String outDir = "hoover.out";

    /**
     * User name for accessing the remote servers.
     */
    @FieldConfig(codable = true)
    private String user;

    /**
     * Path on the remote servers for retrieving files.
     * This is a Unix path, so glob-matching (wildcard-matching) is allowed.
     */
    @FieldConfig(codable = true)
    private String path;

    /**
     * If non-null then only retrieve the files
     * a file path that match this regular expression.
     * Default is null.
     */
    @FieldConfig(codable = true)
    private String match;

    /**
     * Regular expression for extracting the date from the file name or path. Default is "(.*)".
     */
    @FieldConfig(codable = true)
    private String dateMatcher = "(.*)";

    /**
     * The <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html">DateTimeFormat</a>
     * for each date that is extracted from either the file name or the file path. Default is "yyyy-MM-dd".
     */
    @FieldConfig(codable = true)
    private String dateExtractor = "yyyy-MM-dd";

    /**
     * Start date and time for filtering.
     */
    @FieldConfig(codable = true)
    private String startDate;

    /**
     * End date and time for filtering.
     */
    @FieldConfig(codable = true)
    private String endDate;

    /**
     * The <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html">DateTimeFormat</a>
     * for the parameters 'startDate' and 'endDate'. Default is "yyyy-MM-dd-HH".
     */
    @FieldConfig(codable = true)
    private String startEndDateFormat = "yyyy-MM-dd-HH";

    /**
     * Execute this command to fetch the list of files on the remote machine.
     * Default is ["ssh", "{{USER}}@{{HOST}}", "ls", "{{PATH}}" ].
     */
    @FieldConfig(codable = true)
    private String[] listCommand = new String[]{"ssh", "{{USER}}@{{HOST}}", "ls", "{{PATH}}"};

    /**
     * Execute this command to copy a file from the remote machine to the local machine.
     * <br>Default is ["rsync", "-av", "{{USER}}@{{HOST}}:{{REMOTEPATH}}", "{{LOCALPATH}}" ].
     */
    @FieldConfig(codable = true)
    private String[] copyCommand = new String[]{"rsync", "-av", "{{USER}}@{{HOST}}:{{REMOTEPATH}}", "{{LOCALPATH}}"};

    /**
     * Optionally run this command at the completion of the file transfer. Default is null.
     */
    @FieldConfig(codable = true)
    private String[] postCommand;

    /**
     * If this flag is set, then fail this job when the the post command
     * returns a non-zero value and the number of files in the output directory is zero.
     * Default is false.
     */
    @FieldConfig(codable = true)
    private boolean failOnPostIfOutEmpty = false;


    /**
     * Output path and filename for each retrieved file.
     * The final path is constructed by concatenating the
     * elements of the string array with the directory separator ("/").
     * It is a bad idea to exclude "{{FILE}}" from this parameter.
     * Default is ["{{HOST}}-{{FILE}}"]
     */
    @FieldConfig(codable = true)
    private String[] pathOut = new String[]{"{{HOST}}-{{FILE}}"};

    /**
     * If true then {{FILE}} is replaced with the file name.
     * Otherwise {{FILE}} is replaced with the whole path. Default is true.
     */
    @FieldConfig(codable = true)
    private boolean useShortPath = true;

    /**
     * If true then show output of remote commands in the logger. Default is true.
     */
    @FieldConfig(codable = true)
    private boolean verbose = true;

    /**
     * If true then perform gzip compression on the retrieved files. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean compress;

    /**
     * If true then emit additional logging information when
     * testing if a file is to be transferred. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean verboseCheck = false;

    /**
     * If true, log the stderr instead of stdout of the process
     */
    @FieldConfig(codable = true)
    private boolean traceError = false;

    /**
     * Files that are older than this number of days will be deleted.
     */
    @FieldConfig(codable = true)
    private int purgeAfterDays = 30;

    /**
     * Command to be executed to delete files. Default is
     * <br>["find", "{{DIR}}", "-type", "f", "-mtime", "+{{DAYS}}", "-print", "-exec", "rm", "{}", ";"]</br>
     */
    @FieldConfig(codable = true)
    private String[] purgeCommand = new String[]{"find", "{{DIR}}", "-type", "f", "-mtime", "+{{DAYS}}", "-print", "-exec", "rm", "{}", ";"};

    /**
     * If true then purge mark files that are older than purgeAfterDays days. Default is true.
     */
    @FieldConfig(codable = true)
    private boolean purgeMarks = true;

    /**
     * If true then extract the date from the file path.
     * Otherwise extract the date from the file name.
     * Default is false.
     */
    @FieldConfig(codable = true)
    private boolean pathBasedDateMatching = false;

    /**
     * Optional file creator with (key,value) pairs of the format
     * <fileName or fileName;mode> : <"file string contents">
     */
    @FieldConfig(codable = true)
    private HashMap<String, String> staticFiles = new HashMap<>();

    /**
     * Maximum number of attempts to retrieve files from remote hosts. Default is 5.
     */
    @FieldConfig(codable = true)
    private int maxFindAttempts = 5;

    @FieldConfig
    private TaskRunConfig config;

    private AtomicBoolean terminated = new AtomicBoolean(false);
    private SimpleDateFormat dateFormat;
    private Pattern datePattern;
    private Thread thread;
    private Integer[] mods;
    private File markRoot;
    private DateTime jodaStartDate = null;
    private DateTime jodaEndDate = null;
    private final Backoff backoff = new Backoff(1000, 10000);

    public Date parseDate(String input) throws Exception {
        if (dateExtractor.equals("seconds")) {
            return new Date(Long.parseLong(input)*1000);
        } else if (dateExtractor.equals("millis") || dateExtractor.equals("milliseconds")) {
            return new Date(Long.parseLong(input));
        } else {
            if (dateFormat == null) dateFormat = new SimpleDateFormat(dateExtractor);
            return dateFormat.parse(input);
        }
    }

    @Override
    public void init() {
        this.mods = config.calcShardList(config.nodeCount);
        this.markRoot = Files.initDirectory(new File(markDir));
        this.datePattern = Pattern.compile(dateMatcher);
        if (startDate != null) {
            this.jodaStartDate = DateUtil.getDateTime(DateUtil.getFormatter(startEndDateFormat), startDate);
        }
        if (endDate != null) {
            this.jodaEndDate = DateUtil.getDateTime(DateUtil.getFormatter(startEndDateFormat), endDate);
        }

        log.info("init config={} mods={}", config, Strings.join(mods, ","));
        /* create job files from map if not already existing or changed */
        for (Map.Entry<String, String> file : staticFiles.entrySet()) {
            String[] fileName = Strings.splitArray(file.getKey(), ";");
            String mode = fileName.length > 1 ? fileName[1] : "755";
            File out = new File(fileName[0]);
            byte[] raw = Bytes.toBytes(file.getValue());
            if (out.exists() && out.isFile() && out.length() == raw.length) {
                continue;
            }
            try {
                Files.initDirectory(out.getParentFile());
                Files.write(out, raw, false);
                Runtime.getRuntime().exec("chmod " + mode + " " + out).waitFor();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void exec() {
        thread = new Thread(this);
        thread.setName("Hoover Rsync");
        thread.start();
        log.info("exec " + config.jobId);
    }

    @Override
    public void close() {
        if (terminated.compareAndSet(false, true)) {
            thread.interrupt();
            log.info("terminate " + config.jobId);
        }
        try {
            thread.join();
            log.info("exit " + config.jobId);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void run() {
        File outDirFile = new File(outDir);
        if (!outDirFile.exists()) {
            log.info("out dir does not yet exist, creating: " + outDir);
            outDirFile.mkdirs();
        }
        for (Map.Entry<String, String> host : hosts.entrySet()) {
            log.info("fetching from " + host.getValue() + " ...");
            Collection<MarkFile> files;
            int attempts = 0;
            while (true) {
                files = findFiles(host.getKey(), host.getValue());
                if (files == null && attempts++ > maxFindAttempts) {
                    log.error("Unable to find files to hoover after " + attempts + " attempts");
                    throw new RuntimeException("Unable to find files to hoover after " + attempts + " attempts");
                } else if (files == null) {
                    log.warn("error running findFiles command, backing off before retry.  This is attempt: " + attempts);
                    try {
                        Thread.sleep(backoff.get());
                    } catch (InterruptedException e) {
                        if (terminated.get()) {
                            break;
                        }
                    }
                } else {
                    // success case, break retry loop
                    break;
                }
            }

            for (MarkFile mark : files) {
                attempts = 0;
                while (!terminated.get()) {
                    if (fetchFile(mark, host.getValue()) && !terminated.get()) {
                        mark.write();
                        // successfully retrieved and marked file, break retry loop
                        break;
                    } else {
                        log.warn("error fetching " + mark.fileName + " from host: " + mark.host + " on attempt: " + attempts);
                        if (attempts++ < maxFindAttempts && !terminated.get()) {
                            try {
                                Thread.sleep(backoff.get());
                            } catch (InterruptedException e) {
                                if (terminated.get()) {
                                    break;
                                }
                            }
                        } else {
                            log.error("max retry attempts: " + attempts + " breaking retry loop");
                            break;
                        }
                    }
                }
            }

        }
        purgeDir(outDir, purgeAfterDays);
        if (purgeMarks) {
            purgeDir(markDir, purgeAfterDays);
        }
        final int postRet = postCommand();
        if (postRet != 0) {
            if (!terminated.get() && (failOnPostIfOutEmpty && outDirFile.list().length == 0)) {
                log.error("error returned by postCommand, forcing system.exit:" + postRet);
                // We need to exit in a separate thread, because
                // another thread is joining on this one, and
                // inexplicably System.exit does not interrupt blocked
                // threads.  An alternative to System.exit would be
                // ideal, but that would require some more invasive
                // changes.
                new Thread(new Runnable() {
                    public void run() {
                        System.exit(postRet);
                    }
                }, "HooverSystemExiter").start();
                thread.interrupt();
            } else {
                log.warn("error returned by postCommand; taking no action since exit was already in process:" + postRet);
            }
        }
    }

    private MarkFile getMarkFile(String host, String fileName) {
        return new MarkFile(host, fileName);
    }

    private int postCommand() {
        if (postCommand != null && postCommand.length > 0) {
            Process proc = null;
            int attempts = 0;
            // post command retry loop
            while (true) {
                try {
                    proc = Runtime.getRuntime().exec(postCommand);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                    BufferedReader ereader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        if (verbose || log.isDebugEnabled()) log.warn("post --> " + line);
                    }
                    reader.close();
                    int exit = 0;
                    if ((exit = proc.waitFor()) != 0) {
                        log.error("post exited with " + exit);
                        String eline = null;
                        while ((eline = ereader.readLine()) != null) {
                            System.err.println(eline);
                        }
                        return exit;
                    }
                } catch (Exception ex)  {
                    log.error("", ex);
                    if (proc != null) {
                        log.error("Problem during postCommand, destroying process " + ex);
                        proc.destroy();
                    }
                    if (attempts++ < maxFindAttempts && !terminated.get()) {
                        try {
                            Thread.sleep(backoff.get());
                        } catch (InterruptedException e) {
                            // do nothing
                        }
                        continue;
                    } else {
                        log.error("Max retry attempts: " + attempts + " reached.  Post command failed");
                        return 1;
                    }

                }
                // if we get here, success, so break retry loop
                break;
            }
        }
        return 0;
    }


    // TODO: change time for files that are not append only
    /*
     * throw out files if:
     *   - doesn't "match" regex (if set)
     *   - doesn't match mod hash of file name against shard list
     *   - was already fetched in a previous run
     */
    private boolean checkFile(MarkFile markFile) {
        if (match != null && !markFile.path.matches(match)) {
            if (verboseCheck || log.isDebugEnabled()) {
                log.info("match skip for host=" + markFile.host + " path=" + markFile.path + " match=" + match);
            }
            return false;
        }
        if (jodaStartDate != null && markFile.dateTime != null && markFile.dateTime.isBefore(jodaStartDate)) {
            if (verboseCheck || log.isDebugEnabled()) {
                log.info("skipping host=" + markFile.host + " path=" + markFile.path + " because " + markFile.dateTime + " is before startime" + jodaStartDate);
            }
            return false;
        }
        if (jodaEndDate != null && markFile.dateTime != null && markFile.dateTime.isAfter(jodaEndDate)) {
            if (verboseCheck || log.isDebugEnabled()) {
                log.info("skipping host=" + markFile.host + " path=" + markFile.path + " because " + markFile.dateTime + " is after end" + jodaEndDate);
            }
            return false;
        }

        if (markFile.markFile.exists()) {
            if (verboseCheck || log.isDebugEnabled()) log.info("mark skip for host=" + markFile.host + " file=" + markFile.name());
            return false;
        }
        int hashMod = Math.abs(PluggableHashFunction.hash(markFile.host.concat(markFile.name()))) % config.nodeCount;
        for (Integer mod : mods) {
            if (log.isDebugEnabled()) log.debug("mod=" + mod + " hashMod=" + hashMod);
            if (hashMod == mod) {
                return true;
            }
        }
        if (verboseCheck || log.isDebugEnabled()) {
            log.info("hash skip [" + hashMod + "] host=" + markFile.host + " file=" + markFile.name());
        }
        return false;
    }

    /*
     * exec scp to find list of remote files
     */
    private Collection<MarkFile> findFiles(String hostNickname, String host) {
        LinkedList<MarkFile> files = new LinkedList<>();
        String[] newCmd = new String[listCommand.length];
        for (int i = 0; i < newCmd.length; i++) {
            newCmd[i] = listCommand[i].replace("{{USER}}", user).replace("{{HOST}}", host).replace("{{PATH}}", path);
        }
        if (verboseCheck || log.isDebugEnabled()) log.info("find cmd=" + Strings.join(newCmd, " "));
        try {
            Process proc = Runtime.getRuntime().exec(newCmd);
            BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String fileName = null;
            while ((fileName = reader.readLine()) != null) {
                MarkFile markFile = getMarkFile(hostNickname, fileName.trim());
                if (verboseCheck || log.isDebugEnabled()) log.info("found host=" + host + " file=" + fileName);
                if (checkFile(markFile)) {
                    files.add(markFile);
                }
            }
            reader.close();
            proc.waitFor();
        } catch (Exception ex) {
            log.warn("error finding files from host: " + host, ex);
            return null;
        }
        return files;
    }

    private boolean fetchFile(MarkFile markFile, String host) {
        String[] path = new String[pathOut.length];
        for (int i = 0; i < path.length; i++) {
            path[i] = pathOut[i].replace("{{HOST}}", markFile.host).replace("{{FILE}}", markFile.name());
            if (markFile.dateYear != null) {
                path[i] = path[i].replace("{{YY}}", markFile.dateYear);
                path[i] = path[i].replace("{{Y}}", markFile.dateYear.substring(2));
                path[i] = path[i].replace("{{M}}", markFile.dateMonth);
                path[i] = path[i].replace("{{D}}", markFile.dateDay);
                path[i] = path[i].replace("{{H}}", markFile.dateHour);
            }
        }
        String pathString = outDir + "/" + Strings.join(path, "/");
        String[] newCmd = new String[copyCommand.length];
        File fileOut = new File(pathString);
        File fileDir = Files.initDirectory(fileOut.getParentFile());
        if (verboseCheck || log.isDebugEnabled()) log.info("fileDir=" + fileDir + " fileOut=" + fileOut+" outDir="+fileDir+", "+fileDir.exists());
        for (int i = 0; i < newCmd.length; i++) {
            newCmd[i] = copyCommand[i].replace("{{USER}}", user).replace("{{HOST}}", host).replace("{{LOCALPATH}}", pathString).replace("{{REMOTEPATH}}", markFile.path);
        }
        if (log.isDebugEnabled()) log.debug("copy cmd = " + Strings.join(newCmd, " "));
        try {
            Process proc = Runtime.getRuntime().exec(newCmd);
            BufferedReader reader = new BufferedReader(new InputStreamReader(traceError ? proc.getErrorStream() : proc.getInputStream()));
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (verboseCheck || log.isDebugEnabled()) log.info(" --> " + line);
            }
            reader.close();
            int ret = proc.waitFor();
            if (ret != 0) {
                log.warn("non-zero return code ("+ret+") while executing: " + Strings.join(newCmd, " "));
                return false;
            }
            if (compress && !pathString.endsWith(".gz")) {
                File compressTo = new File(pathString + ".gz");
                if (compressTo.exists()) {
                    if (log.isDebugEnabled()) log.debug("" + compressTo + " already exists, deleting and recreating");
                    compressTo.delete();
                }
                if (log.isDebugEnabled()) log.debug("compressing file: " + pathString);
                Process gzipProc = Runtime.getRuntime().exec(new String[]{"gzip", pathString});
                if (gzipProc.waitFor() != 0) {
                    log.warn("non-zero return code while gzipping: " + pathString);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
        if (verbose || log.isDebugEnabled()) {
            log.info("fetched " + markFile.host + " --> " + markFile.name() + " marked by " + markFile.markFile);
        }
        return true;
    }

    private boolean purgeDir(String dir, int days) {
        if (days <= 0) return true;
        String[] newCmd = new String[purgeCommand.length];
        for (int i = 0; i < newCmd.length; i++) {
            newCmd[i] = purgeCommand[i].replace("{{DIR}}", dir).replace("{{DAYS}}", Integer.toString(days));
        }
        if (log.isDebugEnabled()) log.debug("purge cmd = " + Strings.join(newCmd, " "));
        if (verbose || log.isDebugEnabled()) log.info("purging older than days=" + days + " from dir=" + dir);
        try {
            Process proc = Runtime.getRuntime().exec(newCmd);
            BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (verbose || log.isDebugEnabled()) log.info(" --> " + line);
            }
            reader.close();
            return proc.waitFor() == 0;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    /** */
    private class MarkFile {

        public final String host;
        public final String path;
        public final String fileName;
        public final File markFile;
        public final String dateYear;
        public final String dateMonth;
        public final String dateDay;
        public final String dateHour;
        public final DateTime dateTime;

        MarkFile(String host, String path) {
            this.host = host;
            this.path = path;
            this.fileName = new File(path).getName();
            try {
                File hostRoot = Files.initDirectory(new File(markRoot, host));
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                BigInteger val = new BigInteger(1, md5.digest(Bytes.toBytes(path)));
                String hashName = Strings.padleft(val.toString(16), 32, Strings.pad0);
                markFile = new File(hostRoot, hashName);
                Matcher fileMatcher = datePattern.matcher(fileName);
                Matcher pathMatcher = datePattern.matcher(path);
                if (!pathBasedDateMatching && fileMatcher.find(0)) {
                    String group1 = fileMatcher.group(1);
                    Date date = parseDate(group1);
                    String datePrint = dateOut.format(date);
                    if (verboseCheck || log.isDebugEnabled()) {
                        log.info("extract group1=" + group1 + " date=" + date + " datePrint=" + datePrint);
                    }
                    dateYear = datePrint.substring(0, 4);
                    dateMonth = datePrint.substring(4, 6);
                    dateDay = datePrint.substring(6, 8);
                    dateHour = datePrint.substring(8, 10);
                    dateTime = new DateTime(date);
                } else if (pathBasedDateMatching && pathMatcher.find(0)) {
                    String group1 = pathMatcher.group(1);
                    Date date = parseDate(group1);
                    String datePrint = dateOut.format(date);
                    if (verboseCheck || log.isDebugEnabled()) {
                        log.info("extract group1=" + group1 + " date=" + date + " datePrint=" + datePrint);
                    }
                    dateYear = datePrint.substring(0, 4);
                    dateMonth = datePrint.substring(4, 6);
                    dateDay = datePrint.substring(6, 8);
                    dateHour = datePrint.substring(8, 10);
                    dateTime = new DateTime(date);
                } else {
                    dateYear = null;
                    dateMonth = null;
                    dateDay = null;
                    dateHour = null;
                    dateTime = null;
                }
                if (verboseCheck || log.isDebugEnabled()) {
                    log.info("extract "+toString());
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            if (verboseCheck || log.isDebugEnabled()) log.info("mark " + host + " -> " + path + " = " + markFile);
        }

        public String toString() {
            return "dp=" + datePattern + " fn=" + fileName + " dy=" + dateYear + " dm=" + dateMonth + " dd=" + dateDay + " dh=" + dateHour;
        }

        public String name() {
            return useShortPath ? fileName : path;
        }

        public void write() {
            try {
                Files.write(markFile, Bytes.toBytes(name()), false);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
