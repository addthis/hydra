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
package com.addthis.hydra.data.query.channel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.Socket;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

import com.addthis.basis.util.Bytes;

import com.addthis.codec.Codec;
import com.addthis.codec.CodecBin2;
import com.addthis.codec.CodecJSON;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * simple io wrapper for sending/receiving bytes
 */
public class QueryChannel<SEND extends Codec.Codable, RECV extends Codec.Codable> {

    private static final Logger log = LoggerFactory.getLogger(QueryChannel.class);
    private static final int MAGIC = 0x01020304;

    public static final int FLAG_COMPRESS_V1 = 1;

    private final Class<? extends RECV> recvClass;
    private final Codec codec;
    private final InputStream recv;

    private String label;
    private OutputStream send;
    private boolean compressed_v1 = false;
    private Lock recvLock = new ReentrantLock();
    private Lock sendLock = new ReentrantLock();

    public QueryChannel(Socket socket, Class<? extends RECV> recvClass) throws IOException {
        this(socket.getInputStream(), recvClass, socket.getOutputStream(), new CodecBin2(), 0);
        this.label = socket.toString() + " :: ";
    }

    public QueryChannel(Socket socket, Class<? extends RECV> recvClass, int flags) throws IOException {
        this(socket.getInputStream(), recvClass, socket.getOutputStream(), new CodecBin2(), flags);
        this.label = socket.toString() + " :: ";
    }

    private QueryChannel(InputStream in, Class<? extends RECV> recvClass, OutputStream out) throws IOException {
        this(in, recvClass, out, new CodecBin2(), 0);
    }

    private QueryChannel(InputStream in, Class<? extends RECV> recvClass, OutputStream out, int flags) throws IOException {
        this(in, recvClass, out, new CodecBin2(), flags);
    }

    private QueryChannel(InputStream recv, Class<? extends RECV> recvClass, OutputStream send, Codec codec, int flags) throws IOException {
        this.recv = recv;
        this.send = send;
        this.codec = codec;
        this.recvClass = recvClass;
        this.label = "";
        sendFlags(flags);
        recvFlags();
    }

    private final void sendFlags(int flags) throws IOException {
        Bytes.writeInt(MAGIC, send);
        Bytes.writeInt(flags, send);
    }

    private final void recvFlags() throws IOException {
        int magic = Bytes.readInt(recv);
        int flags = Bytes.readInt(recv);
        if (magic != MAGIC) {
            throw new RuntimeException("invalid magic " + magic);
        }
        if ((flags & FLAG_COMPRESS_V1) == FLAG_COMPRESS_V1) {
            compressed_v1 = true;
            send = new GZIPOutputStream(send);
        }
    }

    private void log(Object o1, String join, Object o2, Codec.Codable msg) {
        try {
            String s1 = o1.toString();
            s1 = s1.substring(s1.lastIndexOf(".") + 1);
            String s2 = o2.toString();
            s2 = s2.substring(s2.lastIndexOf(".") + 1);
            String s3 = label.concat(s1).concat(" : ").concat(join).concat(" : ").concat(s2).concat(" : ").concat(CodecJSON.encodeString(msg));
            log.warn(s3);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void sendSync(SEND msg) throws Exception {
        sendLock.lock();
        try {
            send(msg);
        } finally {
            sendLock.unlock();
        }
    }

    private void send(SEND msg) throws Exception {
        if (log.isDebugEnabled()) log(send, ">>>", recv, msg);
        Bytes.writeBytes(codec.encode(msg), send);
    }

    public RECV recvSync() throws Exception {
        recvLock.lock();
        try {
            return recv();
        } finally {
            recvLock.unlock();
        }

    }

    private RECV recv() throws Exception {
        RECV ret = codec.decode(recvClass, Bytes.readBytes(recv));
        if (log.isDebugEnabled()) log(recv, "<<<", send, ret);
        return ret;
    }

    public void flush() {
        try {
            send.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            if (compressed_v1) {
                ((GZIPOutputStream) send).finish();
            }
            send.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            recv.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
