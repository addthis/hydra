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
package com.addthis.hydra.task.stream;

import java.io.OutputStream;

import java.net.Socket;

import com.addthis.basis.util.Bytes;

import com.addthis.codec.Codec;


/**
 * @user-reference
 */
public final class StreamHost implements Codec.Codable {

    /**
     * Host name.
     */
    @Codec.Set(codable = true)
    private String host;

    /**
     * Port number.
     */
    @Codec.Set(codable = true)
    private int port;

    /**
     * Protocol number. 0 for STREAM, 1 for HTTP, 2 for MUX, 3 for MESHY. Default is 0.
     */
    @Codec.Set(codable = true)
    private int protocol;

    // codec constructor
    public StreamHost() {
    }

    public StreamHost(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public String toString() {
        return "StreamHost[" + host + ":" + port + "]";
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public StreamFile.PROTOCOL protocol() {
        return StreamFileNative.decode(protocol);
    }

    public boolean isUp() {
        try {
            Socket s = new Socket(host, port);
            OutputStream out = s.getOutputStream();
            Bytes.writeString("+", out);
            out.flush();
            s.close();
            out.close();
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
}
