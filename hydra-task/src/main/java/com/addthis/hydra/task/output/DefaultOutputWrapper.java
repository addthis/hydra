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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

import com.addthis.bundle.core.Bundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class DefaultOutputWrapper implements OutputWrapper {

    private static final Logger log = LoggerFactory.getLogger(DefaultOutputWrapper.class);

    private OutputStream rawout;
    private OutputStreamEmitter lineout;
    private File tempTargetFile;
    private File targetFile;
    private boolean compress;
    private int compressType;
    private final AtomicLong lineCount = new AtomicLong();
    private long lastAccessTime;
    private String rawTarget;
    private final Lock wrapperLock = new ReentrantLock();
    private boolean closed = false;

    public DefaultOutputWrapper(OutputStream out, OutputStreamEmitter lineout, File targetFile, File tempTargetFile, boolean compress, int compressType, String rawTarget) {
        this.rawout = out;
        this.lineout = lineout;
        this.tempTargetFile = tempTargetFile;
        this.targetFile = targetFile;
        this.compress = compress;
        this.compressType = compressType;
        this.rawTarget = rawTarget;
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
    public String getRawTarget() {
        return rawTarget;
    }

    @Override
    public void incrementLineCount() {
        lineCount.incrementAndGet();
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    @Override
    public long getLineCount() {
        return lineCount.get();
    }

    @Override
    public boolean exceedsSize(long maxSizeInBytes) {
        return (targetFile != null && targetFile.length() > maxSizeInBytes) || (tempTargetFile != null && tempTargetFile.length() > maxSizeInBytes);
    }

    @Override
    public void close() {
        wrapperLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (lineout != null) {
                try {
                    lineout.flush(rawout);
                } catch (Exception ex)  {
                    log.warn("", ex);
                }
            }
            if (rawout != null) {
                if (compress && compressType == 0) {
                    try {
                        ((GZIPOutputStream) rawout).finish();
                    } catch (Exception ex)  {
                        log.warn("", ex);
                    }
                } else if (compress && compressType == 2) {
                    try {
                        rawout.flush();
                    } catch (IOException e)  {
                        log.warn("", e);
                    }
                } else if (compress && compressType == 3) {
                    try {
                        rawout.flush();
                    } catch (IOException e)  {
                        log.warn("", e);
                    }
                }
                closeStream(rawout);
            }
            if (tempTargetFile != null && tempTargetFile != targetFile) {
                if (log.isDebugEnabled()) {
                    log.debug("renaming " + tempTargetFile + " to " + targetFile);
                }
                if (!tempTargetFile.renameTo(targetFile)) {
                    log.warn("Failed to rename " + tempTargetFile + " to " + targetFile);
                }
            }
        } finally {
            wrapperLock.unlock();
        }
    }

    private void closeStream(OutputStream outputStream) {
        try {
            outputStream.flush();
        } catch (Exception ex)  {
            log.warn("", ex);
        } finally {
            try {
                outputStream.close();
            } catch (Exception ex)  {
                log.warn("", ex);
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

    @Override
    public void write(ByteArrayOutputStream bufOut, Bundle row) throws IOException {
        wrapperLock.lock();
        try {
            if (!closed) {
                lineout.write(bufOut, row);
            } else {
                throw new IOException("output wrapper for file: " + targetFile.getName() + " was closed");
            }
        } finally {
            wrapperLock.unlock();
        }
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        wrapperLock.lock();
        try {
            if (!closed) {
                rawout.write(bytes);
            } else {
                throw new IOException("output wrapper for file: " + targetFile.getName() + " was closed");
            }
        } finally {
            wrapperLock.unlock();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void lock() {
        wrapperLock.lock();
    }

    @Override
    public void unlock() {
        wrapperLock.unlock();
    }
}
