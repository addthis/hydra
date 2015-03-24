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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.LessStrings;

import com.addthis.hydra.store.compress.CompressedStream;
import com.addthis.hydra.store.compress.CompressionType;
import com.addthis.muxy.MuxFileDirectory;
import com.addthis.muxy.MuxFileDirectoryCache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

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
public class DefaultOutputWrapperFactory implements OutputWrapperFactory {

    private static final Logger log = LoggerFactory.getLogger(DefaultOutputWrapperFactory.class);

    /** Path to the root directory of the output files. */
    @JsonProperty private String dir;

    /**
     * If true then generate a small number of large files to represent a large number of small files.
     * See comment above. Default is false.
     */
    @JsonProperty private boolean multiplex;

    @JsonProperty private int multiplexMaxFileSize;
    @JsonProperty private int multiplexMaxBlockSize;
    @JsonProperty private int multiplexWriteThashold;
    @JsonProperty private int multiplexLogCloseTimeout;

    /** Used to be required for codec, but now is only required for some old unit tests. */
    @VisibleForTesting
    @Deprecated
    DefaultOutputWrapperFactory() {}

    @JsonCreator
    public DefaultOutputWrapperFactory(@JsonProperty("dir") String dir) {
        this.dir = dir;
    }

    static OutputStream wrapOutputStream(OutputStreamFlags outputFlags,
                                         boolean exists,
                                         OutputStream outputStream) throws IOException {
        OutputStream wrappedStream;
        if (outputFlags.isCompress()) {
            wrappedStream = CompressedStream.compressOutputStream(
                    outputStream, CompressionType.fromOrdinal(outputFlags.getCompressType()));
        } else {
            wrappedStream = new BufferedOutputStream(outputStream);
        }
        if (!exists && (outputFlags.getHeader() != null)) {
            wrappedStream.write(LessBytes.toBytes(outputFlags.getHeader()));
        }
        return wrappedStream;
    }

    static String getFileName(String target,
                              PartitionData partitionData,
                              OutputStreamFlags outputFlags,
                              int fileVersion) {
        // by convention the partition can never be greater than 999
        String result = target;
        if (outputFlags.isNoAppend() || (outputFlags.getMaxFileSize() > 0)) {

            String versionString = Integer.toString(fileVersion);
            checkArgument(versionString.length() <= partitionData.getPadTo(),
                          "fileVersion (%s) cannot be longer than %s digits or else padding will loop; try {{PART:%s}}",
                          fileVersion, partitionData.getPadTo(), versionString.length());
            String part = LessStrings.padleft(versionString, partitionData.getPadTo(), LessStrings.pad0);
            if (partitionData.getReplacementString() != null) {
                result = target.replace(partitionData.getReplacementString(), part);
            } else {
                result = target.concat("-").concat(part);
            }
            if (outputFlags.isCompress()) {
                CompressionType type = CompressionType.fromOrdinal(outputFlags.getCompressType());
                result = result.concat(type.suffix);
            }
        }

        if (outputFlags.isCompress()) {
            CompressionType type = CompressionType.fromOrdinal(outputFlags.getCompressType());
            if (!result.endsWith(type.suffix)) {
                result = result.concat(type.suffix);
            }
        }
        log.debug("[file] compress={} compressType:{} na={} for {}",
                  outputFlags.isCompress(), outputFlags.getCompressType(), outputFlags.isNoAppend(), result);
        return result;
    }

    @Override
    public OutputWrapper openWriteStream(String target,
                                         OutputStreamFlags outputFlags,
                                         OutputStreamEmitter streamEmitter) throws IOException {
        log.debug("[open] {}target={}", outputFlags, target);
        String rawTarget = target;
        target = getModifiedTarget(target, outputFlags);
        File targetOut = new File(dir, target);
        File targetOutTmp = getTempFileName(target);
        File targetParent = targetOut.getParentFile();
        if (!targetParent.exists() && requireDirectory(targetParent) == null) {
            throw new IOException("unable to create target " + target);
        }
        OutputStream outputStream;
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
        return new DefaultOutputWrapper(wrappedStream, streamEmitter, targetOut, targetOutTmp,
                                        outputFlags.isCompress(), outputFlags.getCompressType(), rawTarget);
    }

    @Deprecated protected void setDir(String dir) {
        this.dir = dir;
    }

    private String getModifiedTarget(String target, OutputStreamFlags outputFlags) {
        PartitionData partitionData = PartitionData.getPartitionData(target);
        String modifiedFileName;
        int i = 0;
        while (true) {
            modifiedFileName = getFileName(target, partitionData, outputFlags, i++);
            File test = new File(dir, modifiedFileName);
            File testTmp = getTempFileName(modifiedFileName);
            if ((outputFlags.getMaxFileSize() > 0)
                && ((test.length() >= outputFlags.getMaxFileSize())
                    || (testTmp.length() >= outputFlags.getMaxFileSize()))) {
                // to big already
                continue;
            }
            if (!outputFlags.isNoAppend() || !exists(modifiedFileName, test)) {
                break;
            }
        }
        return modifiedFileName;
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

    @Nullable private static File requireDirectory(File file) throws IOException {
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

    @Nonnull @Override
    public ImmutableList<Path> writableRootPaths() {
        return ImmutableList.of(Paths.get(dir));
    }
}
