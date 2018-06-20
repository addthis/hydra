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
package com.addthis.hydra.minion;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

import com.addthis.basis.util.LessBytes;

import com.addthis.maljson.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogUtils {
    private static final Logger log = LoggerFactory.getLogger(LogUtils.class);
    private static final int LOG_BUF_LIMIT = 20000000;

    /** Streams task log files from newest to oldest. The returned Stream should be closed. */
    public static Stream<Path> streamTaskLogsByName(JobTask task) throws IOException {
        Path logDir = task.logDir.toPath();
        return Files.list(logDir)
                    .filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                    .sorted(Collections.reverseOrder());
    }

    public static Optional<Path> getNthNewestLog(JobTask task, int runsAgo, String suffix) throws IOException {
        // short circuit for the most common case
        if (runsAgo == 0) {
            if ("err".equals(suffix)) {
                return Optional.of(task.logErr.toPath());
            } else if ("out".equals(suffix)) {
                return Optional.of(task.logOut.toPath());
            }
        }
        try (Stream<Path> paths = streamTaskLogsByName(task)
                .filter(path -> path.toString().endsWith(suffix))
                .skip(runsAgo)) {
            return paths.findFirst();
        }
    }

    public static JSONObject readLogLines(JobTask task, int startOffset, int lines, int runsAgo, String suffix) {
        try {
            Optional<Path> logFile = getNthNewestLog(task, runsAgo, suffix);
            if (!logFile.isPresent()) {
                log.info("no log file found for task {}, {} runs ago, with suffix {}", task, runsAgo, suffix);
                return new JSONObject();
            }
            return readLogLines(logFile.get().toFile(), startOffset, lines);
        } catch (Exception ex) {
            log.warn("exception while trying to serve task logs via http", ex);
            return new JSONObject();
        }
    }

    public static JSONObject readLogLines(File file, int startOffset, int lines) {
        JSONObject json = new JSONObject();
        String content = "";
        long off = 0;
        long endOffset = 0;
        int linesRead = 0;
        int bytesRead = 0;
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long len = raf.length();
            //if startoffset is negative, tail the content
            if (startOffset < 0 || startOffset > len) {
                off = len;
                while (lines > 0 && --off >= 0) {
                    raf.seek(off);
                    if (off == 0 || raf.read() == '\n') {
                        lines--;
                        linesRead++;
                    }
                }
                bytesRead = (int) (len - off);
                byte[] buf = readFileBytesLimited(raf, bytesRead);
                content = LessBytes.toString(buf);
                endOffset = len;
            } else if (len > 0 && startOffset < len) {
                off = startOffset;
                while (lines > 0 && off < len) {
                    raf.seek(off++);
                    if (raf.read() == '\n') {
                        lines--;
                        linesRead++;
                    }
                }
                bytesRead = (int) (off - startOffset);
                raf.seek(startOffset);
                byte[] buf = readFileBytesLimited(raf, bytesRead);
                content = LessBytes.toString(buf);
                endOffset = off;
            } else if (startOffset == len) {
                endOffset = len;
                linesRead = 0;
                content = "";
            }
            json.put("offset", endOffset);
            json.put("lines", linesRead);
            json.put("lastModified", file.lastModified());
            json.put("out", content);
        } catch (Exception e) {
            log.warn("", e);
        }
        return json;
    }

    public static String tail(File file, int lines) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long len = raf.length();
            if (len <= 0) {
                return "";
            }
            long off = len;
            while (lines > 0 && --off >= 0) {
                raf.seek(off);
                if (off == 0 || raf.read() == '\n') {
                    lines--;
                }
            }
            byte[] buf = readFileBytesLimited(raf, (int)(len - off));
            return LessBytes.toString(buf);
        } catch (Exception e) {
            log.warn("", e);
        }
        return "";
    }

    public static String head(File file, int lines) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long len = raf.length();
            if (len <= 0) {
                return "";
            }
            long off = 0;
            while (lines > 0 && off < len) {
                raf.seek(off++);
                if (raf.read() == '\n') {
                    lines--;
                }
            }
            raf.seek(0);
            byte[] buf = readFileBytesLimited(raf, (int)off);
            return LessBytes.toString(buf);
        } catch (Exception e) {
            log.warn("", e);
        }
        return "";
    }

    private static void addExceedingMsg(byte[] buf) {
        try {
            String lastLineString = "\nExceeded " + LOG_BUF_LIMIT + " bytes. Reduce requested lines!\n";
            byte[] lastLineBytes = lastLineString.getBytes("UTF-8");
            System.arraycopy(lastLineBytes, 0, buf, buf.length-lastLineBytes.length, lastLineBytes.length);
        } catch  (UnsupportedEncodingException e) {
            log.warn("", e);
        }
    }

    private static byte[] readFileBytesLimited(RandomAccessFile raf, int bytesRead){
        byte[] buf = null;
        try {
            // limiting log reads below 20MB, in case of reaching heap limit and crashing minion
            int limitedBytesRead = Math.min(bytesRead, LOG_BUF_LIMIT);
            buf = new byte[limitedBytesRead];
            raf.read(buf);
            if (bytesRead >= LOG_BUF_LIMIT) {
                addExceedingMsg(buf);
            }
        } catch (IOException e) {
            log.warn("", e);
        }
        return buf;
    }

    private LogUtils() {}
}
