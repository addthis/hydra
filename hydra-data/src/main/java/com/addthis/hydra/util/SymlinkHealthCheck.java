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
package com.addthis.hydra.util;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import com.addthis.basis.util.Parameter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class SymlinkHealthCheck extends CountingHealthCheck {

    private final Path targetDirectory;

    /**
     * default is 10 minutes *
     */
    private static final long SYMLINK_CHECK_INTERVAL =
            Parameter.longValue("symlink.check.interval", 10 * 60 * 1000);

    private static final int SYMLINK_MAX_DEPTH = 64;

    private static final Logger log = LoggerFactory.getLogger(SymlinkHealthCheck.class);

    public SymlinkHealthCheck(Path targetDirectory) {
        super(1, "SymlinkDuplicateFailure", true);
        this.targetDirectory = targetDirectory;
    }

    private Path resolveLink(Path path) throws IOException {
        int depth;

        for (depth = 0; depth < SYMLINK_MAX_DEPTH && Files.isSymbolicLink(path); depth++) {
            path = Files.readSymbolicLink(path);
        }

        if (depth == SYMLINK_MAX_DEPTH) {
            return null;
        } else {
            return path;
        }
    }

    public final HealthCheckThread startHealthCheckThread() {
        return this.startHealthCheckThread(SYMLINK_CHECK_INTERVAL, "symlinkHealthCheck");
    }

    @Override
    public boolean check() {
        /** a map of sinks back to their sources **/
        Map<Path, Path> symlinkMap = new HashMap<>();

        try {
            if (!Files.exists(targetDirectory)) {
                return true;
            }

            if (!Files.isDirectory(targetDirectory)) {
                return true;
            }

            DirectoryStream<Path> stream = Files.newDirectoryStream(targetDirectory);

            for (Path source : stream) {
                Path sink = resolveLink(source);

                if (sink == null) {
                    String msg = "The symlink " + source + " has a recursive depth of" +
                                 " greater than " + SYMLINK_MAX_DEPTH;
                    log.warn(msg);
                    return false;
                }

                sink = sink.toAbsolutePath();

                if (symlinkMap.containsKey(sink)) {
                    Path firstSource = symlinkMap.get(sink);
                    String msg = "The symlinks " + firstSource + " and " + source;
                    msg += " both resolve to the destination " + sink;
                    log.warn(msg);
                    return false;
                }
                symlinkMap.put(sink, source);
            }

        } catch (IOException e) {
            log.warn(e.getMessage());
            return false;
        }

        return true;
    }
}
