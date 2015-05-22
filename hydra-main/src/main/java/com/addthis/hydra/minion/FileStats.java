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

import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStats {
    private static final Logger log = LoggerFactory.getLogger(FileStats.class);

    public long count;
    public long bytes;

    void update(File dir) {
        try {
            Files.walkFileTree(dir.toPath(), new SimpleFileVisitor<Path>() {
                @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    count += 1;
                    bytes += attrs.size();
                    return super.visitFile(file, attrs);
                }
            });
        } catch (IOException e) {
            log.warn("Exception while scanning task files; treating directory as empty", e);
            count = 0;
            bytes = 0;
        }
    }
}
