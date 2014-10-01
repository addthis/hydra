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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.OutputStream;

public class DefaultOutputWrapper extends AbstractOutputWrapper {

    private static final Logger log = LoggerFactory.getLogger(DefaultOutputWrapper.class);


    private File tempTargetFile;
    private File targetFile;


    public DefaultOutputWrapper(OutputStream out, OutputStreamEmitter lineout, File targetFile, File tempTargetFile, boolean compress, int compressType, String rawTarget) {
        super(out, lineout, compress, compressType, rawTarget);
        this.tempTargetFile = tempTargetFile;
        this.targetFile = targetFile;
    }

    @Override
    public String getFileName() {
        String result = "";
        if (targetFile != null) {
            result = targetFile.getPath() + "/" + targetFile.getName();
        }
        return result;
    }

    @Override
    public boolean exceedsSize(long maxSizeInBytes) {
        return (targetFile != null && targetFile.length() > maxSizeInBytes) || (tempTargetFile != null && tempTargetFile.length() > maxSizeInBytes);
    }


    @Override
    protected void renameTempTargetFile() {
        if (tempTargetFile != null && tempTargetFile != targetFile) {
            if (log.isDebugEnabled()) {
                log.debug("renaming " + tempTargetFile + " to " + targetFile);
            }
            if (!tempTargetFile.renameTo(targetFile)) {
                log.warn("Failed to rename " + tempTargetFile + " to " + targetFile);
            }
        }
    }

    @Override
    public String toString() {
        return "DefaultOutputWrapper{" + "targetFile=" + targetFile + ", tempTargetFile=" + tempTargetFile + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DefaultOutputWrapper that = (DefaultOutputWrapper) o;

        if (targetFile != null ? !targetFile.equals(that.targetFile) : that.targetFile != null) {
            return false;
        }
        if (tempTargetFile != null ? !tempTargetFile.equals(that.tempTargetFile) : that.tempTargetFile != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = tempTargetFile != null ? tempTargetFile.hashCode() : 0;
        result = 31 * result + (targetFile != null ? targetFile.hashCode() : 0);
        return result;
    }
}
