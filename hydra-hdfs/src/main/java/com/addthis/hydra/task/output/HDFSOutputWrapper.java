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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An output wrapper for HDFS files.
 */
public class HDFSOutputWrapper extends AbstractOutputWrapper {
    private final static Logger log = LoggerFactory.getLogger(HDFSOutputWrapper.class);

    private final Path tempTargetPath;
    private final Path targetPath;
    private final FileSystem fileSystem;

    public HDFSOutputWrapper(OutputStream out, OutputStreamEmitter lineout,
                             boolean compress, int compressType, String rawTarget,
                             Path targetPath, Path tempTargetPath, FileSystem fileSystem) {
        super(out, lineout, compress, compressType, rawTarget);
        this.targetPath = targetPath;
        this.tempTargetPath = tempTargetPath;
        this.fileSystem = fileSystem;
    }

    @Override
    protected void renameTempTargetFile() {
        if (tempTargetPath != null && tempTargetPath != targetPath) {
            if (log.isDebugEnabled()) {
                log.debug("renaming " + tempTargetPath.toUri() + " to " + targetPath.toUri());
            }
            try {
                if (!fileSystem.rename(tempTargetPath, targetPath)) {
                    log.warn("Failed to rename " + tempTargetPath.toUri() + " to " + targetPath.toUri());
                }
            } catch (IOException e) {
                log.warn("Failed to rename " + tempTargetPath.toUri() + " to " + targetPath.toUri());
            }
        }
    }

    @Override
    public String getFileName() {
        return targetPath.toUri().toString();
    }

    @Override
    public boolean exceedsSize(long maxSizeInBytes) {
        try {
            return ((targetPath != null && fileSystem.exists(targetPath) && fileSystem.getFileStatus(targetPath).getLen() > maxSizeInBytes) ||
                    (tempTargetPath != null && fileSystem.exists(tempTargetPath) && fileSystem.getFileStatus(tempTargetPath).getLen() > maxSizeInBytes));
        } catch (IOException e)  {
            log.warn("", e);
            return false;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HDFSOutputWrapper that = (HDFSOutputWrapper) o;

        if (targetPath != null ? !targetPath.equals(that.targetPath) : that.targetPath != null) {
            return false;
        }
        if (tempTargetPath != null ? !tempTargetPath.equals(that.tempTargetPath) : that.tempTargetPath != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = tempTargetPath != null ? tempTargetPath.hashCode() : 0;
        result = 31 * result + (targetPath != null ? targetPath.hashCode() : 0);
        return result;
    }


    @Override
    public String toString() {
        return "HDFSOutputWrapper{" + "targetFile=" + getFileName() + ", tempTargetFile=" + tempTargetPath.toUri().toString() + '}';
    }
}
