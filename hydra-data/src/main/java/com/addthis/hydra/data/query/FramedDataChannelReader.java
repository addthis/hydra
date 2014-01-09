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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.Bytes;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.bundle.io.DataChannelReader;
import com.addthis.meshy.service.stream.SourceInputStream;

public class FramedDataChannelReader extends DataChannelReader {

    public AtomicBoolean eof = new AtomicBoolean(false);
    public boolean busy;
    private DataChannelError err;
    private final SourceInputStream in;
    public final String fileReferenceName;
    private final DataChannelCodec.ClassIndexMap classMap;
    private final DataChannelCodec.FieldIndexMap fieldMap;
    private ByteArrayInputStream bis;
    private final int pollWaitTime;

    public static final int FRAME_MORE = 0;
    public static final int FRAME_EOF = 1;
    public static final int FRAME_ERROR = 2;
    public static final int FRAME_BUSY = 3;

    public FramedDataChannelReader(final SourceInputStream in, String fileReferenceName, int pollWaitTime) {
        this(in, fileReferenceName, DataChannelCodec.createClassIndexMap(), DataChannelCodec.createFieldIndexMap(), pollWaitTime);
    }

    public FramedDataChannelReader(final SourceInputStream in, String fileReferenceName,
            DataChannelCodec.ClassIndexMap classMap, DataChannelCodec.FieldIndexMap fieldMap, int pollWaitTime) {
        super(new ListBundle(), in);
        this.in = in;
        this.fileReferenceName = fileReferenceName;
        this.classMap = classMap;
        this.fieldMap = fieldMap;
        this.pollWaitTime = pollWaitTime;
    }

    public int available() throws IOException {
        return in.available();
    }

    @Override
    public Bundle read() throws IOException {
        if (eof.get()) {
            return null;
        }
        if (err != null) {
            throw err;
        }
        int frame;
        if (bis != null && bis.available() > 0) {
            frame = bis.read();
        } else {
            byte[] data = in.poll(pollWaitTime, TimeUnit.MILLISECONDS);
            if (data == null) {
                // poll timeout no data yet
                return null;
            } else if (data.length == 0) {
                eof.set(true);
                return null;
//              // 0 byte array from client, probably error in mesh stream
//              Not working as I had expected commenting out for now
//              err = new DataChannelError("[FrameDataChannelReader] received unexpected zero length byte array");
//              throw err;
            } else {
                // more data to read
                bis = new ByteArrayInputStream(data);
                frame = bis.read();
            }
        }

        switch (frame) {
            case FRAME_BUSY:
                busy = true;
                return null;
            case FRAME_MORE:
                return DataChannelCodec.decodeBundle(getFactory().createBundle(), Bytes.readBytes(bis), fieldMap, classMap);
            case FRAME_EOF:
                eof.set(true);
                return null;
            case FRAME_ERROR:
                try {
                    String error = Bytes.readString(bis);
                    String errorMessage = Bytes.readString(bis);
                    Class clazz = Class.forName(error);
                    err = (DataChannelError) clazz.getConstructor(String.class).newInstance(errorMessage);
                    throw err;
                } catch (Exception ex) {
                    if (ex instanceof DataChannelError) {
                        throw (DataChannelError) ex;
                    } else {
                        throw new DataChannelError(ex);
                    }
                }
            default:
                throw new DataChannelError("invalid framing: " + frame);
        }
    }
}
