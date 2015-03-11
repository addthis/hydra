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
package com.addthis.hydra.job.minion;

import java.io.File;
import java.io.RandomAccessFile;

import com.addthis.basis.util.LessBytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWatcher {
    private static final Logger log = LoggerFactory.getLogger(FileWatcher.class);

    private File file;
    private long lastSize;
    private long lastEnd;
    private RandomAccessFile access;
    private String needle;

    FileWatcher(File file, String needle) {
        try {
            this.access = new RandomAccessFile(file, "r");
            this.needle = needle;
            this.file = file;
        } catch (Exception ex) {
            log.warn("", ex);
        }
    }

    public boolean containsKill() {
        log.warn("containsKill(" + file + "," + access + ")");
        if (access != null) {
            try {
                long len = file.length();
                if (len > lastSize) {
                    lastSize = len;
                    // find last CR
                    log.warn("searching " + lastEnd + " to " + len);
                    for (long pos = len - 1; pos >= lastEnd; pos--) {
                        access.seek(pos);
                        if (access.read() == '\n') {
                            long start = lastEnd;
                            long size = pos - start;
                            if (size > 32768) {
                                log.warn("[warning] skipping search @ " + size);
                                lastEnd = pos;
                                return false;
                            } else if (size > 4096) {
                                log.warn("[warning] searching > 4k space @ " + size);
                            }
                            byte[] scan = new byte[(int) size];
                            access.seek(start);
                            access.readFully(scan);
                            log.warn("scan of " + LessBytes.toString(scan));
                            lastEnd = pos;
                            return (LessBytes.toString(scan).indexOf(needle) >= 0);
                        }
                    }
                }
            } catch (Exception ex) {
                log.warn("", ex);
            }
        }
        return false;
    }
}
