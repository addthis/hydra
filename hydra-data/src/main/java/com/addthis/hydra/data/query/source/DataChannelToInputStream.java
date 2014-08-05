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

package com.addthis.hydra.data.query.source;

import javax.annotation.concurrent.GuardedBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.core.BundleFormatted;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.io.DataChannelWriter;
import com.addthis.hydra.data.query.FieldValueList;
import com.addthis.hydra.data.query.FramedDataChannelReader;
import com.addthis.hydra.data.query.QueryElement;
import com.addthis.hydra.data.query.engine.QueryEngine;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.meshy.VirtualFileInput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelProgressivePromise;

/**
 * This class is the last point the bundles reach before getting converted into bytes and read by meshy to be
 * sent over the network to the client (MQMaster). The last class in the three step query process.
 */
class DataChannelToInputStream implements DataChannelOutput, VirtualFileInput, BundleFormatted {

    static final Logger log = LoggerFactory.getLogger(DataChannelToInputStream.class);

    private static final int outputQueueSize = Parameter.intValue("meshQuerySource.outputQueueSize", 1000);
    private static final int outputBufferSize = Parameter.intValue("meshQuerySource.outputBufferSize", 64000);
    private static final int queueAttemptLimit = Parameter.intValue("meshQuerySource.queueAttemptLimit", 100);

    private final ListBundleFormat format = new ListBundleFormat();
    private final LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>(outputQueueSize);
    private final DataChannelWriter writer;
    private final ByteArrayOutputStream out;
    /**
     * A wrapper for a boolean flag that gets set if close is called. This observer object will be passed all
     * the way down to {@link QueryEngine#tableSearch(LinkedList,
     * DataTreeNode, FieldValueList, QueryElement[], int, DataChannelOutput, int, ChannelProgressivePromise)}.
     */
    public final ChannelProgressivePromise queryPromise;
    private int rows = 0;
    /**
     * A boolean flag that gets set to true once all the data have been sent to the stream, and not necessarily
     * pulled or read from the stream.
     */
    private volatile boolean eof;

    /**
     * Checks whether or not {@link #closed} was set to true. If {@link #closed} has been set to true then
     * it means there has either been a failure up stream or the query has been canceled.
     * In either case we want to stop the running of this stream and at that point this function will throw
     * a DataChannelError exception.
     */
    private volatile boolean closed = false;

    /**
     * A non-public constructor. This class can only be instantiated from it outer class MeshQueryMaster. The objects
     * can be accessed elsewhere using the interfaces.
     *
     * @throws Exception
     * @param queryPromise
     */
    DataChannelToInputStream(ChannelProgressivePromise queryPromise) throws Exception {
        this.queryPromise = queryPromise;
        out = new ByteArrayOutputStream();
        writer = new DataChannelWriter(out);
    }

    /**
     * This function is called by meshy and it convers the data pushed to the out ByteArrayOutputStream into
     * byte[], which gets read by meshy and sent over the channel to the source.
     *
     * @param wait the amount of milliseconds to wail polling for data.
     * @return a byte array if data exists in the OutputStream otherwise returns null if the timeout expires.
     */
    @Override
    public byte[] nextBytes(long wait) {
        if (closed || isEOF()) {
            log.debug("EOF reached. rows={}", rows);
            return null;
        }
        byte[] data;
        try {
            data = queue.poll(wait, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
            log.warn("Interrupted while waiting for data");
            eof = true;
            return null;
        }
        if (data == null) {
            synchronized (out) {
                if (out.size() > 0) {
                    emitChunks();
                }
            }
            data = queue.poll();
        }
        return data;
    }

    /**
     * Must only be called while synchronized on out
     */
    @GuardedBy("out")
    private void emitChunks() {
        if (out.size() == 0) {  // this should never be true
            return;
        }
        byte[] bytes = out.toByteArray();
        out.reset();
        try {
            for (int i = 0; i < queueAttemptLimit; i++) //Try adding to queue queueAttemptLimit times
            {
                if (queue.offer(bytes, 1000L, TimeUnit.MILLISECONDS)) {
                    if (i > 10) {
                        log.warn("Managed to add to output queue on the {} attempt", i);
                    }
                    return;
                }
                if (closed) {
                    log.info("Unable to emit chunks due to closed channel");
                    break;
                }
            }
            throw new DataChannelError("Output queue oversized for " + queueAttemptLimit + " + attempts");
        } catch (InterruptedException e) {
            log.warn("Interrupted while putting bytes onto output buffer");
            throw new DataChannelError("interrupted", e);
        }
    }

    /**
     * Returns true if the eof flag is set and there is no data queued in the stream to be sent.
     *
     * @return true if EOF otherwise false.
     */
    @Override
    public boolean isEOF() {
        return eof && queue.isEmpty();
    }

    /**
     * Tells this channel to close. It sets the closed flag and the queryPromise to true.
     */
    @Override
    public void close() {
        closed = true;
        queryPromise.cancel(false);
    }

    /**
     * Takes in a list of bundles, loops on them and calls send for each one.
     *
     * @param bundles
     */
    @Override
    public void send(List<Bundle> bundles) {
        // Just in case the list was empty, we check if the channel is closed here
        if (closed) {
            log.debug("Unable to send bundles due to closed channel");
            throw new CancellationException("master cancelled query");
        }
        for (Bundle bundle : bundles) {
            send(bundle);
        }
    }

    /**
     * Takes in a bundle and writes it on the writer (mapped to out),
     * which encodes the bundle to bytes.
     *
     */
    @Override
    public void send(Bundle bundle) throws DataChannelError {
        if (closed) {
            log.debug("Unable to send bundles due to closed channel");
            throw new CancellationException("master cancelled query");
        }
        try {
            synchronized (out) {
                out.write(FramedDataChannelReader.FRAME_MORE);
                writer.write(bundle);
                if (out.size() > outputBufferSize) {
                    emitChunks();
                }
                rows++;
            }
        } catch (IOException ex) {
            throw new DataChannelError(ex);
        }
    }

    /**
     * Is called when all the data has been sent. It sets the eof flag and writes an EOF marker on the output
     * stream.
     */
    @Override
    public void sendComplete() {
        if (closed) {
            log.debug("Unable to send complete due to closed channel");
            throw new CancellationException("master cancelled query");
        }
        synchronized (out) {
            out.write(FramedDataChannelReader.FRAME_EOF);
            emitChunks();
            eof = true;
        }
    }

    /**
     * This function gets called when an error is encountered from the source.
     *
     * @param er error encountered from the source
     */
    @Override
    public void sourceError(Throwable er) {
        if (closed) {
            log.debug("Unable to send source error due to closed channel", er);
            return;
        }
        try {
            // if we know writer is closed, don't try to write to it.
            if (!writer.isClosed()) {
                synchronized (out) {
                    out.write(FramedDataChannelReader.FRAME_ERROR);
                    Bytes.writeString(er.getClass().getCanonicalName(), out);
                    Bytes.writeString(er.getMessage(), out);
                    emitChunks();
                    eof = true;
                }
            }
        } catch (Exception ex) {
            throw new DataChannelError(ex);
        }
    }

    @Override
    public Bundle createBundle() {
        return new ListBundle(format);
    }

    @Override
    public BundleFormat getFormat() {
        return format;
    }

    /**
     * @return a reference to the {@link #queryPromise}.
     */
    public ChannelProgressivePromise getQueryPromise() {
        return queryPromise;
    }
}
