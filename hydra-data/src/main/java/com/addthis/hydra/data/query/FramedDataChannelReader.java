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
package com.addthis.hydra.data.query;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Bytes;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.io.BundleReader;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.hydra.data.util.BundleUtils;
import com.addthis.meshy.service.stream.StreamSource;

import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class FramedDataChannelReader implements BundleReader {

    private static final Logger log = LoggerFactory.getLogger(FramedDataChannelReader.class);

    public static final int FRAME_MORE = 0;
    public static final int FRAME_EOF = 1;
    public static final int FRAME_ERROR = 2;
    public static final int FRAME_BUSY = 3;

    private final StreamSource streamSource;
    private final DataChannelCodec.ClassIndexMap classMap;
    private final DataChannelCodec.FieldIndexMap fieldMap;
    private final BundleFactory factory;
    private final BlockingQueue<byte[]> queue;
    private final int pollWaitTime;

    private ByteArrayInputStream bis;
    private DataChannelError err;
    private boolean eof;

    public FramedDataChannelReader(StreamSource streamSource, int pollWaitTime) {
        this(streamSource, pollWaitTime,
             DataChannelCodec.createClassIndexMap(), DataChannelCodec.createFieldIndexMap());
    }

    public FramedDataChannelReader(StreamSource streamSource, int pollWaitTime,
                                   DataChannelCodec.ClassIndexMap classMap,
                                   DataChannelCodec.FieldIndexMap fieldMap) {
        this.streamSource = streamSource;
        this.pollWaitTime = pollWaitTime;
        this.classMap = classMap;
        this.fieldMap = fieldMap;

        this.queue = streamSource.getMessageQueue();
        this.factory = new ListBundle();
        this.eof = false;
    }

    @Override @Nullable public Bundle read() throws IOException {
        if (err != null) {
            throw err;
        }
        if (eof) {
            return null;
        }
        if ((bis == null) || (bis.available() == 0)) {
            byte[] data;
            try {
                data = queue.poll(pollWaitTime, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
            handlePollResult(data);
            if (eof) {
                return null;
            } else if (data == null) {
                // poll timeout no data yet
                return null;
            } else {
                // more data to read
                bis = new ByteArrayInputStream(data);
            }
        }

        int frame = bis.read();
        switch (frame) {
            case FRAME_BUSY:
                err = new DataChannelError("busy frames are not supported");
                throw err;
            case FRAME_MORE:
                return DataChannelCodec.decodeBundle(factory.createBundle(), Bytes.readBytes(bis), fieldMap, classMap);
            case FRAME_EOF:
                close();
                return null;
            case FRAME_ERROR:
                try {
                    String error = Bytes.readString(bis);
                    String errorMessage = Bytes.readString(bis);
                    Class clazz = Class.forName(error);
                    err = (DataChannelError) clazz.getConstructor(String.class).newInstance(errorMessage);
                } catch (DataChannelError ex) {
                    err = ex;
                    throw ex;
                } catch (Exception ex) {
                    throw new DataChannelError(ex);
                }
                throw err;
            default:
                err = new DataChannelError("invalid framing: " + frame);
                throw err;
        }
    }

    private void handlePollResult(byte[] data) throws IOException {
        streamSource.performBufferAccounting(data);
        try {
            streamSource.throwIfErrorSignal(data);
        } catch (Throwable sourceError) {
            // kind of silly, but I just feel like being defensive at the moment
            err = BundleUtils.promoteHackForThrowables(sourceError);
            throw sourceError;
        }
        if (streamSource.isCloseSignal(data)) {
            // maybe this should be an error? leaving it as is for now to mainting existing behavior
            log.warn("bundle stream ended without eof frame");
            close();
        }
    }

    @Override public void close() throws IOException {
        eof = true;
    }

    public boolean isClosed() {
        return eof;
    }

    @Override public BundleFactory getFactory() {
        return factory;
    }
}
