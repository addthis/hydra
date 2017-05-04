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
import java.io.IOException;
import java.io.OutputStream;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.store.compress.CompressionType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOutputWrapper implements OutputWrapper{

    private static final Logger log = LoggerFactory.getLogger(AbstractOutputWrapper.class);

    private OutputStream rawout;
    private OutputStreamEmitter lineout;
    private boolean compress;
    private CompressionType compressType;
    private final AtomicLong lineCount = new AtomicLong();
    private long lastAccessTime;
    private String rawTarget;
    private final Lock wrapperLock = new ReentrantLock();
    private boolean closed = false;

    public AbstractOutputWrapper(OutputStream out, OutputStreamEmitter lineout, boolean compress, CompressionType compressType, String rawTarget) {
        this.rawout = out;
        this.lineout = lineout;
        this.compress = compress;
        this.compressType = compressType;
        this.rawTarget = rawTarget;
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
                if (compress && compressType == CompressionType.GZIP) {
                    try {
                        ((GZIPOutputStream) rawout).finish();
                    } catch (Exception ex)  {
                        log.warn("", ex);
                    }
                } else if (compress && compressType == CompressionType.SNAPPY) {
                    try {
                        rawout.flush();
                    } catch (IOException e)  {
                        log.warn("", e);
                    }
                } else if (compress && compressType == CompressionType.BZIP2) {
                    try {
                        rawout.flush();
                    } catch (IOException e)  {
                        log.warn("", e);
                    }
                }
                closeStream(rawout);
            }
            renameTempTargetFile();
        } finally {
            wrapperLock.unlock();
        }
    }

    protected abstract void renameTempTargetFile();

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
    public void write(ByteArrayOutputStream bufOut, Bundle row) throws IOException {
        wrapperLock.lock();
        try {
            if (!closed) {
                lineout.write(bufOut, row);
            } else {
                throw new IOException("output wrapper for file: " + getFileName() + " was closed");
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
                throw new IOException("output wrapper for file: " + getFileName() + " was closed");
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
