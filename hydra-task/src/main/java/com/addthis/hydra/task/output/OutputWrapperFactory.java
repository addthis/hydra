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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.nio.file.NoSuchFileException;

import com.addthis.basis.io.IOWrap;
import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Strings;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.muxy.MuxFileDirectory;
import com.addthis.muxy.MuxFileDirectoryCache;

import com.ning.compress.lzf.LZFOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import lzma.streams.LzmaOutputStream;

/**
 * <p>Options for file layout within the file system when writing to an output sink.
 * <p/>
 * You should always set the {@link #multiplex multiplex} parameter to <code>true</code> in new split jobs.
 * This parameter uses the Muxy project to write a small number of large files that can
 * represent a very large number of small files.  This reduces the pressure on
 * the OS to maintain the file handles and improves performance.  Multiplexing also
 * helps with replication because the number of files to transfer is
 * directly proportional to the efficiency of the rsyncs we run to
 * replicate data in the cluster.  Think about a split job that has 256 shards and the data is stored by hour.
 * In that case each day of data has a minimum of 2400 files.  That is a lot of overhead for rsync to deal with.
 * Using multiplex would reduce the minimum number of files from 2400 to 24.
 * <p>Example:</p>
 * <pre>factory : {
 *   dir : "split",
 *   multiplex : true,
 * }</pre>
 *
 * @user-reference
 */
public class OutputWrapperFactory {

    private static final Logger log = LoggerFactory.getLogger(OutputWrapperFactory.class);

    private static final int DEFAULT_PADDING = 3;
    protected static final String PART_PREFIX = "{{PART";
    private static final String PART_POSTFIX = "}}";
    private static final int BUFFER_SIZE = 4096;

    /**
     * Path to the root directory of the output files.
     */
    @FieldConfig(codable = true)
    private String dir;

    /**
     * If true then generate a small number of large files
     * to represent a large number of small files.
     * See comment above. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean multiplex;

    @FieldConfig(codable = true)
    private int multiplexMaxFileSize;

    @FieldConfig(codable = true)
    private int multiplexMaxBlockSize;

    @FieldConfig(codable = true)
    private int multiplexWriteThashold;

    @FieldConfig(codable = true)
    private int multiplexLogCloseTimeout;

    /**
     * to support Codable
     */
    public OutputWrapperFactory() {
    }

    public OutputWrapperFactory(String dir) {
        this.dir = dir;
    }

    public OutputWrapper openWriteStream(String target,
                                         OutputStreamFlags outputFlags,
                                         OutputStreamEmitter logLineOutput) throws IOException {
        log.debug("[open] {}target={}", outputFlags, target);
        String rawTarget = target;
        target = getModifiedTarget(target, outputFlags);
        File targetOut = new File(dir, target);
        File targetOutTmp = getTempFileName(target);
        File targetParent = targetOut.getParentFile();
        if (!targetParent.exists() && requireDirectory(targetParent) == null) {
            throw new IOException("unable to create target " + target);
        }
        OutputStream outputStream = null;
        if (multiplex || MuxFileDirectory.isMuxDir(targetParent.toPath())) {
            try {
                MuxFileDirectory mfm = MuxFileDirectoryCache.getWriteableInstance(targetParent);
                if (multiplexMaxFileSize > 0) {
                    mfm.setMaxFileSize(multiplexMaxFileSize);
                }
                if (multiplexMaxBlockSize > 0) {
                    mfm.setMaxBlockSize(multiplexMaxBlockSize);
                }
                if (multiplexWriteThashold > 0) {
                    mfm.setWriteThrashold(multiplexWriteThashold);
                }
                if (multiplexLogCloseTimeout > 0) {
                    mfm.setLazyLogClose(multiplexLogCloseTimeout);
                }
                outputStream = mfm.openFile(targetOut.getName(), true).append();
                targetOutTmp = null;
            } catch (IOException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        } else {
            if (targetOut.exists()) {
                log.debug("[open.append]{}/{} renaming to {}/{}",
                          targetOut, targetOut.exists(), targetOutTmp, targetOutTmp.exists());
                if (!targetOut.renameTo(targetOutTmp)) {
                    throw new IOException("Unable to rename " + targetOut + " to " + targetOutTmp);
                }
            }
            outputStream = new FileOutputStream(targetOutTmp, true);
        }
        OutputStream wrappedStream = wrapOutputStream(outputFlags, targetOut.exists(), outputStream);
        return new DefaultOutputWrapper(wrappedStream, logLineOutput, targetOut, targetOutTmp, outputFlags.isCompress(),
                                        outputFlags.getCompressType(), rawTarget);
    }

    private OutputStream wrapOutputStream(OutputStreamFlags outputFlags, boolean exists, OutputStream outputStream) throws IOException {
        OutputStream wrappedStream;
        if (outputFlags.isCompress()) {
            if (outputFlags.getCompressType() == 0) {
                wrappedStream = IOWrap.gz(outputStream, BUFFER_SIZE);
            } else if (outputFlags.getCompressType() == 1) {
                wrappedStream = new LZFOutputStream(outputStream);
            } else if (outputFlags.getCompressType() == 2) {
                wrappedStream = new SnappyOutputStream(outputStream);
            } else if (outputFlags.getCompressType() == 3) {
                wrappedStream = new BZip2CompressorOutputStream(outputStream);
            } else if (outputFlags.getCompressType() == 4) {
                wrappedStream = new LzmaOutputStream.Builder(outputStream).useMediumDictionarySize().build();
            } else {
                throw new IOException("Unknown compression type: " + outputFlags.getCompressType());
            }
        } else {
            wrappedStream = IOWrap.buffer(outputStream, BUFFER_SIZE);
        }
        if (!(outputFlags.getHeader() == null) || exists) {
            wrappedStream.write(Bytes.toBytes(outputFlags.getHeader()));
        }
        return wrappedStream;
    }

    protected void setDir(String dir) {
        this.dir = dir;
    }

    private String getModifiedTarget(String target, OutputStreamFlags outputFlags) {
        PartitionData partitionData = getPartitionData(target);
        String modifiedFileName;
        int i = 0;
        while (true) {
            modifiedFileName = getFileName(target, partitionData, outputFlags, i++);
            File test = new File(dir, modifiedFileName);
            File testTmp = getTempFileName(modifiedFileName);
            if (outputFlags.getMaxFileSize() > 0 && (test.length() >= outputFlags.getMaxFileSize() || testTmp.length() >= outputFlags.getMaxFileSize())) {
                // to big already
                continue;
            }
            if (!outputFlags.isNoAppend() || !exists(modifiedFileName, test)) {
                break;
            }
        }
        return modifiedFileName;
    }

    private String getFileName(String target, PartitionData partitionData, OutputStreamFlags outputFlags, int fileVersion) {
        // by convention the partition can never be greater than 1000
        String result = target;
        if (outputFlags.isNoAppend() || outputFlags.getMaxFileSize() > 0) {

            String part = Strings.padleft(Integer.toString(fileVersion), partitionData.getPadTo(), Strings.pad0);
            if (partitionData.getReplacementString() != null) {
                result = target.replace(partitionData.getReplacementString(), part);
            } else {
                result = target.concat("-").concat(part);
            }
            if (outputFlags.isCompress()) {
                if (outputFlags.getCompressType() == 0) {
                    result = result.concat(".gz");
                } else if (outputFlags.getCompressType() == 1) {
                    result = result.concat(".lzf");
                } else if (outputFlags.getCompressType() == 2) {
                    result = result.concat(".snappy");
                } else if (outputFlags.getCompressType() == 3) {
                    result = result.concat(".bz2");
                } else if (outputFlags.getCompressType() == 4) {
                    result = result.concat(".lzma");
                } else {
                    throw new RuntimeException("unexpected compressionType: " + outputFlags.getCompressType());
                }
            }
        }

        if (outputFlags.isCompress()) {
            if (outputFlags.getCompressType() == 0 && !result.endsWith(".gz")) {
                result = result.concat(".gz");
            } else if (outputFlags.getCompressType() == 1 && !result.endsWith(".lzf")) {
                result = result.concat(".zlf");
            } else if (outputFlags.getCompressType() == 2 && !result.endsWith(".snappy")) {
                result = result.concat(".snappy");
            } else if (outputFlags.getCompressType() == 3 && !result.endsWith(".bz2")) {
                result = result.concat(".bz2");
            } else if (outputFlags.getCompressType() == 4 && !result.endsWith(".lzma")) {
                result = result.concat(".lzma");
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("[file] compress=" + outputFlags.isCompress() + " compressType:" + outputFlags.getCompressType() + " na=" + outputFlags.isNoAppend() + " for " + result);
        }
        return result;
    }

    private boolean exists(String fileName, File file) {
        boolean exists = false;
        if (multiplex) {
            try {
                MuxFileDirectory mfm = MuxFileDirectoryCache.getWriteableInstance(file.getParentFile());
                exists = mfm.exists(fileName.substring(fileName.lastIndexOf("/") + 1));
            } catch (NoSuchFileException nsfe) {
                return false;
            } catch (Exception e) {
                log.warn("Error testing to see if multiplexed file: " + dir + "/" + fileName + " exists", e);
                }
        } else {
            exists = file.exists();
        }
        return exists;
    }

    private File getTempFileName(String fileName) {
        return new File(dir, fileName.concat(".tmp"));
    }

    private PartitionData getPartitionData(String target) {
        String replacement = null;
        int padTo = DEFAULT_PADDING;
        int startPartitionIndex = target.indexOf(PART_PREFIX);
        if (startPartitionIndex >= 0) {
            int closePartitionIndex = target.indexOf(PART_POSTFIX);
            if (closePartitionIndex > startPartitionIndex) {
                replacement = target.substring(startPartitionIndex, closePartitionIndex + 2);
                String[] tok = Strings.splitArray(target.substring(startPartitionIndex + 2, closePartitionIndex), ":");
                if (tok.length > 1) {
                    padTo = Integer.parseInt(tok[1]);
                }
            }
        }
        return new PartitionData(replacement, padTo);
    }

    protected static class PartitionData {

        private String replacementString;
        private int padTo;

        PartitionData(String replacementString, int padTo) {
            this.replacementString = replacementString;
            this.padTo = padTo;
        }

        public String getReplacementString() {
            return replacementString;
        }

        public int getPadTo() {
            return padTo;
        }
    }

    private static File requireDirectory(File file) throws IOException {
        if (file.isDirectory()) {
            return file;
        }
        if (file.isFile()) {
            return null;
        }
        if (!file.mkdirs()) {
            return null;
        }
        return file;
    }
}
