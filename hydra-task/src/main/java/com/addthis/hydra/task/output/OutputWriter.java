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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.JitterClock;

import com.addthis.codec.annotations.FieldConfig;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * <p>Specifies configuration parameters for writing output to files.
 * <p>Example:</p>
 * <pre>writer : {
 *   maxOpen : 1024,
 *   flags : {
 *     maxSize : "64M",
 *     compress : true,
 *   },
 *   factory : {
 *     dir : "split",
 *     multiplex : true,
 *   },
 *   format : {
 *     type : "channel",
 *   },
 * }</pre>
 *
 * @user-reference
 */
public class OutputWriter extends AbstractOutputWriter {

    private static final Logger log = LoggerFactory.getLogger(OutputWriter.class);

    private static final String DEFAULT_SPLIT_OUTPUT_DIR = "split";

    /**
     * Configuration flags for writing to files.
     * This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private OutputStreamFlags flags;

    /**
     * Options for file layout within the file system.
     * This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private OutputWrapperFactory factory;

    /**
     * Maximum number of files that can be open
     * at any time. Default is 320.
     */
    @FieldConfig(codable = true)
    private int maxOpen = 320;

    @FieldConfig(codable = true)
    private String outputList;

    private File modifiedFileTracker;

    private final ConcurrentHashMap<String, OutputWrapper> openOutputs = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<String> openOutputQueue = new ConcurrentLinkedQueue<>();

    private final Lock outputCreateLock = new ReentrantLock();

    private final Counter closes = Metrics.newCounter(OutputWriter.class, "closes");

    public OutputWriter() throws InterruptedException {
        if (flags == null) {
            flags = new OutputStreamFlags(true, true, 10000L, (250 * (1024L * 1024L)), null);
        }
        if (factory == null) {
            factory = new DefaultOutputWrapperFactory(DEFAULT_SPLIT_OUTPUT_DIR);
        }
        if (outputList != null) {
            modifiedFileTracker = new File(outputList);
            if (modifiedFileTracker.exists()) {
                if (!modifiedFileTracker.delete()) {
                    log.warn("Unable to delete modifiedFileTracker, file may contain stale data");
                    return;
                }
                modifiedFileTracker = new File(outputList);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("OutputWriter initialized with max openFiles: " + maxOpen);
        }

    }

    @Override
    public void open() {
        super.open();
        Metrics.newGauge(OutputWriter.class, "openOutputsGauge", new Gauge<Integer>() {
            @Override
            public Integer value() {
                return openOutputs.size();
            }
        });

        // thread to close open outputs if the maxopen value has been exceeded
        writerMaintenanceThread.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                while (openOutputs.size() > maxOpen) {
                    String fileNameToClose = openOutputQueue.poll();
                    if (fileNameToClose != null) {
                        OutputWrapper outputWrapper = openOutputs.get(fileNameToClose);
                        outputWrapper.lock();
                        try {
                            close(openOutputs.get(fileNameToClose));
                        } finally {
                            outputWrapper.unlock();
                        }
                    }
                }
            }
        }, 500, 500, TimeUnit.MILLISECONDS);
    }

    /**
     * called by multiple thread consumers of the input queue. must be thread
     * safe.
     */
    @Override
    protected boolean dequeueWrite(List<WriteTuple> outputTuples) throws IOException {
        if (outputTuples == null || outputTuples.size() == 0) {
            return false;
        }
        ByteArrayOutputStream bufOut = new ByteArrayOutputStream();
        for (WriteTuple writeTuple : outputTuples) {
            String fileName = writeTuple.fileName;
            // need a loop here to make sure we get a outputwrapper that is open
            // and ready for write
            while (true) {
                OutputWrapper out = getOutputWrapperForFile(fileName);
                out.lock();
                try {
                    if (out.isClosed()) {
                        // another thread closed this wrapper before we got the lock
                        // try to acquire it again
                        continue;
                    }
                    bufOut.reset();
                    out.write(bufOut, writeTuple.bundle);
                    if (bufOut.size() == 0) {
                        if (log.isDebugEnabled()) {
                            log.debug("skipping empty line " + bufOut.size());
                        }
                    } else {
                        out.write(bufOut.toByteArray());
                    }
                    out.incrementLineCount();
                    out.setLastAccessTime(JitterClock.globalTime());
                    break;
                } finally {
                    out.unlock();
                }
            }


        }
        return true;
    }

    @Override
    protected void doCloseOpenOutputs() {
        int closed = 0;
        for (OutputWrapper outputWrapper : openOutputs.values()) {
            outputWrapper.lock();
            try {
                close(outputWrapper);
                closed++;
            } catch (Exception e) {
                log.error("Error closing output " + outputWrapper.toString(), e);
            } finally {
                outputWrapper.unlock();
            }
        }
        log.info("closed " + closed + " open outputs");

    }

    private OutputWrapper getOutputWrapperForFile(String fileName) throws IOException {
        OutputWrapper out = openOutputs.get(fileName);
        if ((out != null)
            && ((out.getLineCount() % 1000) == 0)
            && (flags.getMaxFileSize() > 0)
            && out.exceedsSize(flags.getMaxFileSize())) {
            close(out);
            out = null;
        }
        // the retry code here is an attempt to work around a overlapping file lock exception that is thrown
        // on occasion.  We do not understand why that exception is thrown and the correct solution
        // is to gain that understanding and fix the root cause.  So this is a band-aid.
        int retries = 0;
        while (out == null && retries < 10) {
            outputCreateLock.lock();
            try {
                // first check again to make sure it still doesn't exist
                out = openOutputs.get(fileName);
                if (out == null) {
                    out = factory.openWriteStream(fileName, flags, format != null ? format.createEmitter() : null);
                    if (outputList != null) {
                        markModifiedFile(out.getFileName());
                    }
                    OutputWrapper existingOut = openOutputs.putIfAbsent(fileName, out);
                    if (existingOut != null) {
                        out = existingOut;
                    } else {
                        openOutputQueue.add(fileName);
                    }
                }
            } catch (IOException e) {
                log.warn("exception getting output stream for file: " + fileName +
                         ". This is attempt: " + retries + " will try up to 10 times before failing", e);
                retries++;
                out = null;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }

            } finally {
                outputCreateLock.unlock();
            }
        }
        return out;
    }

    private void markModifiedFile(String fileName) {
        try {
            Files.write(modifiedFileTracker, Bytes.toBytes(fileName + "\n"), true);
        } catch (IOException e) {
            log.error("IOException saving modified files", e);
        }
    }

    private boolean close(OutputWrapper outputWrapper) {
        synchronized (this) {
            outputWrapper.close();
            openOutputs.remove(outputWrapper.getRawTarget());
            openOutputQueue.remove(outputWrapper.getRawTarget());
            closes.inc();
            return true;
        }
    }

    public OutputWriter setOutputWrapperFactory(OutputWrapperFactory factory) {
        this.factory = factory;
        return this;
    }

    public OutputWriter setMaxOpen(int maxOpen) {
        this.maxOpen = maxOpen;
        return this;
    }
}
