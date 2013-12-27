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

import com.addthis.basis.util.Numbers;

import com.addthis.codec.Codec;

/**
 * Specifies configuration flags for writing output to files.
 * <p/>
 * <p>For a time it was recommended that split jobs use {@link #noAppend noAppend} set to <code>true</code>.
 * Now there only one use case where this is required. It is when we need to guarantee that
 * generated files will never change. This is the case when we create data files for consumption by
 * third parties. They need to know that once they've downloaded a file they never need to do so again.
 * In other cases noAppend should be set to <code>false</code>. This means that if new data is received
 * at a later time to the same output location then data is appended to an existing file rather
 * than creating a new file. This prevents the explosion in the number of files maintained
 * and makes the entire system more efficient.
 * <p>Example:</p>
 * <pre>flags : {
 *   maxSize : "64M",
 *   compress : true,
 *   noAppend : false,
 * }</pre>
 *
 * @user-reference
 */
public class OutputStreamFlags implements Codec.SuperCodable {

    /**
     * If true then compress the output files. Default is false.
     */
    @Codec.Set(codable = true)
    private boolean compress;

    /**
     * If compress is true then specify the compression type.
     * 0 for gzip, 1 for lzf, 2 for snappy, 3 for bzip2, or 4 for lzma.
     */
    @Codec.Set(codable = true)
    private int compressType;

    /**
     * If true then do not append output to existing files. Default is false.
     */
    @Codec.Set(codable = true)
    private boolean noAppend;

    @Codec.Set(codable = true)
    private long graceTime;

    /**
     * Optionally specify max file size in bytes. Default is 0.
     */
    @Codec.Set(codable = true)
    private long maxFileSize;

    /**
     * Optionally specify max file size as human-readable text (eg 128MB). Default is null.
     */
    @Codec.Set(codable = true)
    private String maxSize;

    /**
     * Optionally write the following header at the top of the output file. Default is null.
     */
    @Codec.Set(codable = true)
    private String header;

    public static final int WRITE_COMPRESS = 1 << 0;
    public static final int WRITE_NOAPPEND = 1 << 2;

    /**
     * default constructor to support codable
     */
    @SuppressWarnings({"UnusedDeclaration"})
    public OutputStreamFlags() {
    }

    public OutputStreamFlags(int flags) {
        this(flags, null);
    }

    public OutputStreamFlags(int flags, String header) {
        this((flags & WRITE_COMPRESS) == WRITE_COMPRESS, (flags & WRITE_NOAPPEND) == WRITE_NOAPPEND, Math.max(0, ((flags >> 16) & 0xff) * 60000L),
                ((flags >> 24) & 0xff) * (1024L * 1024L), header);
    }

    public OutputStreamFlags(boolean compress, boolean noAppend, long graceTimeMillis, long maxFileSizeBytes, String header) {
        this.compress = compress;
        this.noAppend = noAppend;
        this.graceTime = graceTimeMillis;
        this.maxFileSize = maxFileSizeBytes;
        this.header = header;
    }

    public boolean isCompress() {
        return compress;
    }

    public int getCompressType() {
        return compressType;
    }

    public boolean isNoAppend() {
        return noAppend;
    }

    public long getGraceTime() {
        return graceTime;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public String getHeader() {
        return header;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OutputStreamFlags that = (OutputStreamFlags) o;

        if (compress != that.compress) {
            return false;
        }
        if (graceTime != that.graceTime) {
            return false;
        }
        if (maxFileSize != that.maxFileSize) {
            return false;
        }
        if (noAppend != that.noAppend) {
            return false;
        }
        if (compressType != that.compressType) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (compress ? 1 : 0);
        result = 31 * result + (noAppend ? 1 : 0);
        result = 31 * result + (int) (graceTime ^ (graceTime >>> 32));
        result = 31 * result + (int) (maxFileSize ^ (maxFileSize >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "[localOutputStream.arg] " + "gz=" + compress + ", na=" + noAppend + ", hd=" + (header != null) + ", grace=" + graceTime + ", limit=" + maxFileSize + '}';
    }

    @Override
    public void postDecode() {
        if (maxSize != null) {
            maxFileSize = Numbers.parseHumanReadable(maxSize);
        }
    }

    @Override
    public void preEncode() {
    }
}
