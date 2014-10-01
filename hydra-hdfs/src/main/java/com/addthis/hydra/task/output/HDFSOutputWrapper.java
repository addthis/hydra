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
package com.addthis.hydra.task.output;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.OutputStream;

import java.util.Objects;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Objects.toStringHelper;

/**
 * An output wrapper for HDFS files.
 */
public class HDFSOutputWrapper extends AbstractOutputWrapper {
    private static final Logger log = LoggerFactory.getLogger(HDFSOutputWrapper.class);

    // !---- These are _not_ Java's NIO2 interfaces. ----!
    @Nonnull private final Path tempTargetPath;
    @Nonnull private final Path targetPath;
    @Nonnull private final FileSystem fileSystem;

    public HDFSOutputWrapper(OutputStream out,
                             OutputStreamEmitter lineout,
                             boolean compress,
                             int compressType,
                             String rawTarget,
                             @Nonnull Path targetPath,
                             @Nonnull Path tempTargetPath,
                             @Nonnull FileSystem fileSystem) {
        super(out, lineout, compress, compressType, rawTarget);
        this.targetPath = targetPath;
        this.tempTargetPath = tempTargetPath;
        this.fileSystem = fileSystem;
    }

    @Override
    protected void renameTempTargetFile() {
        if (tempTargetPath != targetPath) {
            log.debug("renaming {} to {}", tempTargetPath.toUri(), targetPath.toUri());
            try {
                if (!fileSystem.rename(tempTargetPath, targetPath)) {
                    log.warn("Failed to rename {} to {}", tempTargetPath.toUri(), targetPath.toUri());
                }
            } catch (IOException e) {
                log.warn("Failed to rename {} to {}", tempTargetPath.toUri(), targetPath.toUri(), e);
            }
        }
    }

    @Override
    public String getFileName() {
        return targetPath.toString();
    }

    @Override
    public boolean exceedsSize(long maxSizeInBytes) {
        try {
            return pathExceedsSize(targetPath, maxSizeInBytes) || pathExceedsSize(tempTargetPath, maxSizeInBytes);
        } catch (IOException e)  {
            log.warn("", e);
            return false;
        }
    }

    private boolean pathExceedsSize(Path path, long maxSizeInBytes) throws IOException {
        return fileSystem.exists(path) && (fileSystem.getFileStatus(path).getLen() > maxSizeInBytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof HDFSOutputWrapper)) {
            return false;
        }

        HDFSOutputWrapper that = (HDFSOutputWrapper) obj;
        return targetPath.equals(that.targetPath) &&
               tempTargetPath.equals(that.tempTargetPath) &&
               fileSystem.equals(that.fileSystem);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tempTargetPath, targetPath);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("tempTargetPath", tempTargetPath)
                .add("targetPath", targetPath)
                .toString();
    }
}
