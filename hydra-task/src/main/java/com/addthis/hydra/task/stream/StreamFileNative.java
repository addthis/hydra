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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.net.URL;

import com.addthis.basis.io.IOWrap;

import com.addthis.codec.Codec;

import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.util.LZFFileInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import org.xerial.snappy.SnappyInputStream;

import lzma.sdk.lzma.Decoder;
import lzma.streams.LzmaInputStream;


public class StreamFileNative implements StreamFile, Codec.SuperCodable {

    public static PROTOCOL decode(int protocol) {
        switch (protocol) {
            case 0:
                return PROTOCOL.STREAM;
            case 1:
                return PROTOCOL.HTTP;
            case 2:
                return PROTOCOL.MUX;
            case 3:
                return PROTOCOL.MESHY;
            default:
                return PROTOCOL.STREAM;
        }
    }

    public static int decode(PROTOCOL proto) {
        switch (proto) {
            case STREAM:
                return 0;
            case HTTP:
                return 1;
            case MUX:
                return 2;
            case MESHY:
                return 3;
            default:
                return 0;
        }
    }

    @Codec.Set(codable = true)
    private String name;
    @Codec.Set(codable = true)
    private long length;
    @Codec.Set(codable = true)
    private long lastModified;
    @Codec.Set(codable = true)
    private StreamHost host;
    @Codec.Set(codable = true)
    private int protocol;

    private File file;
    private PROTOCOL proto = PROTOCOL.STREAM;

    public void postDecode() {
        proto = decode(protocol);
    }

    public void preEncode() {
        protocol = decode(proto);
    }

    public StreamFileNative() {
    }

    public StreamFileNative(File file) {
        this(file.getPath(), file.length(), file.lastModified(), null);
        this.file = file;
    }

    public StreamFileNative(String name, long length, long lastModified, StreamHost host) {
        this.name = name;
        this.length = length;
        this.lastModified = lastModified;
        this.host = host;
    }

    public File file() {
        if (file == null) {
            file = new File(name);
        }
        return file;
    }

    /**
     * Interface
     */
    public String name() {
        return name;
    }

    /**
     * Interface
     */
    public long length() {
        return length;
    }

    /**
     * Interface
     */
    public long lastModified() {
        return lastModified;
    }

    public StreamHost host() {
        return host;
    }

    /**
     * Interface
     */
    public String getPath() {
        return file().getPath();
    }

    public String getURLString() {
        String path = file().getPath();
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (host != null) {
            switch (proto) {
                case MESHY:
                    throw new UnsupportedOperationException("meshy not implemented");
                case MUX:
                    throw new UnsupportedOperationException("mux not implemented");
                case HTTP:
                    return "http://".concat(host.host()).concat(":").concat(Integer.toString(host.port())).concat("/").concat(path);
                case STREAM:
                default:
                    return "stream://".concat(host.host()).concat(":").concat(Integer.toString(host.port())).concat("/").concat(path);
            }
        } else {
            return "file://".concat(path);
        }
    }

    /**
     * Interface
     */
    public InputStream getInputStream() throws IOException {
        InputStream in;
        switch (proto) {
            case HTTP:
                String url = getURLString();
                in = new URL(url).openStream();
                if (url.endsWith(".gz")) {
                    in = IOWrap.gz(in, 4096);
                } else if (url.endsWith(".lzf")) {
                    in = new LZFInputStream(in);
                } else if (url.endsWith(".snappy")) {
                    in = new SnappyInputStream(in);
                } else if (url.endsWith(".bz2")) {
                    in = new BZip2CompressorInputStream(in, true);
                } else if (url.endsWith(".lzma")) {
                    in = new LzmaInputStream(in, new Decoder());
                }
                break;
            case MUX:
                in = null; // TODO remove fix compile
                // TODO
                break;
            case STREAM:
            default:
                if (host != null) {
                    in = StreamFileUtils.openStream(host, name, 0);
                    if (name.endsWith(".gz")) {
                        in = IOWrap.gz(in, 4096);
                    } else if (name().endsWith(".lzf")) {
                        in = new LZFInputStream(in);
                    } else if (name().endsWith(".snappy")) {
                        in = new SnappyInputStream(in);
                    } else if (name().endsWith(".bz2")) {
                        in = new BZip2CompressorInputStream(in, true);
                    } else if (name().endsWith(".lzma")) {
                        in = new LzmaInputStream(in, new Decoder());
                    }
                } else {
                    /* this is the path StreamServer calls b/c no host set */
                    if (name().endsWith(".lzf")) {
                        in = new LZFFileInputStream(new File(name));
                    } else if (name().endsWith(".snappy")) {
                        in = new SnappyInputStream(new FileInputStream(new File(name)));
                    } else if (name().endsWith(".bz2")) {
                        in = new BZip2CompressorInputStream(new FileInputStream(new File(name)));
                    } else if (name().endsWith(".lzma")) {
                        in = new LzmaInputStream(new FileInputStream(new File(name)), new Decoder());
                    } else {
                        in = IOWrap.fileIn(new File(name), 4096, name.endsWith(".gz"));
                    }
                }
                break;
        }
        return in;
    }

    public StreamFile setProtocol(PROTOCOL protocol) {
        this.proto = protocol;
        preEncode();
        return this;
    }

    public StreamFile setProtocol(int proto) {
        this.protocol = proto;
        postDecode();
        return this;
    }

    public String toString() {
        return name + "," + length + "," + lastModified;
    }
}
