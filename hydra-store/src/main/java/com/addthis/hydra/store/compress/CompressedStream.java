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
package com.addthis.hydra.store.compress;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.zip.GZIPOutputStream;

import com.addthis.basis.io.GZIPInputStreamX;

import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.LZMAInputStream;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.github.luben.zstd.ZstdOutputStream;
import com.github.luben.zstd.ZstdInputStream;

public class CompressedStream {

    private static final int BUFFER_SIZE = 4096;

    public static @Nonnull InputStream decompressInputStream(@Nonnull InputStream in,
                                                             @Nonnull String name) throws IOException {
        if (name.endsWith(CompressionType.GZIP.suffix)) {
            in = new GZIPInputStreamX(in, BUFFER_SIZE);
        } else if (name.endsWith(CompressionType.LZF.suffix)) {
            in = new LZFInputStream(in);
        } else if (name.endsWith(CompressionType.SNAPPY.suffix)) {
            in = new SnappyInputStream(in);
        } else if (name.endsWith(CompressionType.ZSTD.suffix)) {
            in = new ZstdInputStream(in);
        } else if (name.endsWith(CompressionType.BZIP2.suffix)) {
            in = new BZip2CompressorInputStream(in, true);
        } else if (name.endsWith(CompressionType.LZMA.suffix)) {
            in = new LZMAInputStream(in);
        } else if (name.endsWith(CompressionType.XZ.suffix)) {
            in = new XZInputStream(in);
        }
        return in;
    }

    public static @Nonnull OutputStream compressOutputStream(@Nonnull OutputStream out,
                                                             @Nonnull CompressionType type) throws IOException {
        switch (type) {
            case GZIP:
                out = new GZIPOutputStream(out, BUFFER_SIZE);
                break;
            case LZF:
                out = new LZFOutputStream(out);
                break;
            case SNAPPY:
                out = new SnappyOutputStream(out);
                break;
            case ZSTD:
                out = new ZstdOutputStream(out);
                break;
            case BZIP2:
                out = new BZip2CompressorOutputStream(out);
                break;
            case LZMA:
                throw new UnsupportedOperationException(
                        "Writing .lzma files is no longer supported. Use .xz format instead");
            case XZ:
                out = new XZOutputStream(out, new LZMA2Options());
                break;
            default:
                throw new IllegalStateException("Unknown compression type " + type);
        }
        return out;
    }

}
