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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.InetAddress;
import java.net.Socket;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;

import com.google.common.collect.MapMaker;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public class StreamFileUtils {

    private static final Logger log = LoggerFactory.getLogger(StreamFileUtils.class);

    private static final Map<String, InetAddress> inetAddressMap = new MapMaker().makeMap();

    private static final int socketTimeout = Parameter.intValue("streamserver.read.timeout", 15 * 60 * 1000);

    public static String replaceGoldWithLiveDotDotGold(String input) {
        // Function name of the year!
        if (input != null && input.contains("/gold/")) {
            input = input.replace("/gold/", "/live/../gold/");
        }
        return input;
    }

    public static String padleft(String str, int len) {
        final String pad = "0000000000";
        if (str.length() < len) {
            return pad.substring(pad.length() - len + str.length()).concat(str);
        } else if (str.length() > len) {
            return str.substring(0, len);
        } else {
            return str;
        }
    }

    static Socket getSocketForHost(StreamHost host) throws IOException {
        InetAddress address = inetAddressMap.get(host.host());
        if (address == null) {
            address = InetAddress.getByName(host.host());
            inetAddressMap.put(host.host(), address);
        }
        return new Socket(address, host.port());
    }

    /**
     * client utility to get input stream from streamserver
     */
    public static InputStream openStream(String host, int port, StreamFile file, long offset) throws IOException {
        return openStream(host, port, file.name(), offset);
    }

    /**
     * client utility to get input stream from streamserver
     */
    public static InputStream openStream(String host, int port, String path, long offset) throws IOException {
        return openStream(new StreamHost(host, port), path, offset);
    }

    /**
     * client utility to get input stream from streamserver
     */
    public static InputStream openStream(StreamHost host, String path, long offset) throws IOException {
        try {
            Socket socket = StreamFileUtils.getSocketForHost(host);
            socket.setSoTimeout(socketTimeout);
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            OutputStream out = socket.getOutputStream();
            Bytes.writeString(path, out);
            Bytes.writeLong(offset, out);
            out.flush();
            return socket.getInputStream();
        } catch (IOException e) {
            log.warn("[stream.server] openStream failed to connect to stream host: " + host.host() + ":" + host.port());
            throw new IOException(e);
        }
    }

    /**
     * client utility to get file find input stream from streamserver
     */
    public static List<StreamFile> findStreams(StreamHost host, String directory, int mod, String regex[]) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("opening socket to host: " + host.host() + ":" + host.port());
        }
        Socket socket = getSocketForHost(host);
        socket.setSoTimeout(socketTimeout);
        OutputStream out = socket.getOutputStream();
        Bytes.writeString("@" + directory, out);
        Bytes.writeInt(mod, out);
        Bytes.writeInt(regex != null ? regex.length : 0, out);
        if (regex != null && regex.length > 0) {
            for (String m : regex) {
                Bytes.writeString(m, out);
            }
        }
        out.flush();
        InputStream in = socket.getInputStream();
        int files = Bytes.readInt(in);
        ArrayList<StreamFile> list = new ArrayList<>(files);
        for (int i = 0; i < files; i++) {
            String fname = Bytes.readString(in);
            long flen = Bytes.readLong(in);
            long fmod = Bytes.readLong(in);
            list.add(new StreamFileNative(fname, flen, fmod, host));
        }
        in.close();
        return list;
    }

    /**
     * client utility to get file find input stream from streamserver
     */
    public static List<StreamFile> findStreams(String host, int port, String directory, String regex[]) throws IOException {
        return findStreams(host, port, directory, 0, regex);
    }

    /**
     * client utility to get file find input stream from streamserver
     */
    public static List<StreamFile> findStreams(String host, int port, String directory, int mod, String regex[]) throws IOException {
        return findStreams(new StreamHost(host, port), directory, mod, regex);
    }

}
