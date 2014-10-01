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

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.task.output.DefaultOutputWrapperFactory.getFileName;
import static com.addthis.hydra.task.output.DefaultOutputWrapperFactory.wrapOutputStream;
import static com.addthis.hydra.task.output.PartitionData.getPartitionData;

/**
 * OutputWrapperFactory implementation for HDFS systems.
 * <p>Example:</p>
 * <pre>writer : {
 *   maxOpen : 1024,
 *   flags : {
 *     maxSize : "64M",
 *     compress : true,
 *   },
 *   factory : {
 *     dir : "split",
 *     type: "hdfs",
 *     hdfsURL:"hdfs://hadoop-name-node:8020",
 *   },
 *   format : {
 *     type : "channel",
 *   },
 * }</pre>
 */
public class HDFSOutputWrapperFactory implements OutputWrapperFactory {
    private static final Logger log = LoggerFactory.getLogger(HDFSOutputWrapperFactory.class);

    /** Path to the root directory of the output files. */
    private final Path dir;
    private final FileSystem fileSystem;

    @JsonCreator
    public HDFSOutputWrapperFactory(@JsonProperty(value = "hdfsUrl", required = true) String hdfsUrl,
                                    @JsonProperty(value = "dir", required = true) Path dir) throws IOException {
        Configuration config = new Configuration();
        config.set("fs.defaultFS", hdfsUrl);
        config.set("fs.automatic.close", "false");
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        this.fileSystem = FileSystem.get(config);
        this.dir = dir;
    }

    /**
     * Opens a write stream for an HDFS output. Most of the complexity in this
     * method is related to determining the correct file name based on the given
     * {@code target} parameter.  If the file already exists and we are appending
     * to an existing file then we will rename that file and open up a new stream which
     * will append data to  that file.  If the file does not exist a new file is created
     * with a .tmp extension.  When the stream is closed the file will be renamed to remove
     * the .tmp extension
     *
     * @param target - the base file name of the target output stream
     * @param outputFlags - output flags setting various options about the output stream
     * @param streamEmitter - the emitter that can convert bundles into the desired byte arrays for output
     * @return a OutputWrapper which can be used to write bytes to the new stream
     * @throws IOException propagated from underlying components
     */
    @Override
    public OutputWrapper openWriteStream(String target,
                                         OutputStreamFlags outputFlags,
                                         OutputStreamEmitter streamEmitter) throws IOException {
        log.debug("[open] {}target={} hdfs", outputFlags, target);
        String rawTarget = target;
        target = getModifiedTarget(target, outputFlags);
        Path targetPath = new Path(dir, target);
        Path targetPathTmp = new Path(dir, target.concat(".tmp"));
        boolean exists = fileSystem.exists(targetPath);
        FSDataOutputStream outputStream;
        if (exists) {
            log.debug("[open.append]{}/ renaming to {}/{}",
                      targetPath, targetPathTmp, fileSystem.exists(targetPathTmp));
            if (!fileSystem.rename(targetPath, targetPathTmp)) {
                throw new IOException("Unable to rename " + targetPath.toUri() + " to " + targetPathTmp.toUri());
            }
            outputStream = fileSystem.append(targetPathTmp);
        } else {
            outputStream = fileSystem.create(targetPathTmp, false);
        }
        OutputStream wrappedStream = wrapOutputStream(outputFlags, exists, outputStream);
        return new HDFSOutputWrapper(wrappedStream, streamEmitter, outputFlags.isCompress(),
                                     outputFlags.getCompressType(), rawTarget, targetPath, targetPathTmp, fileSystem);
    }

    private String getModifiedTarget(String target, OutputStreamFlags outputFlags) throws IOException {
        PartitionData partitionData = getPartitionData(target);
        String modifiedFileName;
        int i = 0;
        while (true) {
            modifiedFileName = getFileName(target, partitionData, outputFlags, i++);
            Path test = new Path(dir, modifiedFileName);
            Path testTmp = new Path(dir, modifiedFileName.concat(".tmp"));
            boolean testExists = fileSystem.exists(test);
            if (outputFlags.getMaxFileSize() > 0 && (testExists && fileSystem.getFileStatus(test).getLen() >= outputFlags.getMaxFileSize() || (fileSystem.exists(testTmp) && fileSystem.getFileStatus(testTmp).getLen() >= outputFlags.getMaxFileSize()))) {
                // to big already
                continue;
            }
            if (!outputFlags.isNoAppend() || !testExists) {
                break;
            }
        }
        return modifiedFileName;
    }
}
