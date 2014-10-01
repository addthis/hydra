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
import java.io.IOException;
import java.io.OutputStream;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.task.output.DefaultOutputWrapperFactory.getFileName;
import static com.addthis.hydra.task.output.PartitionData.getPartitionData;
import static com.addthis.hydra.task.output.DefaultOutputWrapperFactory.wrapOutputStream;

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

    /** URL for HDFS name node. */
    @JsonProperty(required = true) private String hdfsURL;

    /** Path to the root directory of the output files. */
    @JsonProperty private String dir;

    /** When true the file name will include the task id. */
    @JsonProperty private boolean includeTaskId = true;

    private final Lock fileSystemInitLock = new ReentrantLock();
    private boolean fileSystemInitialized = false;
    private FileSystem fileSystem;

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
        fileSystemInitLock.lock();
        try {
            if (!fileSystemInitialized) {
                File workingDirectory = new File("tmp");
                String path = workingDirectory.getAbsolutePath();
                String[] tokens = path.split("/");
                if (tokens.length > 3 && includeTaskId) {
                    // include job id and task id in path
                    dir = tokens[tokens.length - 4] + "/" + tokens[tokens.length - 3] + "/" + dir;
                } else if (tokens.length > 3) {
                    // include job id only in path
                    dir = tokens[tokens.length - 4] + "/" + dir;
                }
                fileSystemInitialized = true;
                Configuration config = new Configuration();
                config.set("fs.defaultFS", hdfsURL);
                config.set("fs.automatic.close", "false");
                config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
                fileSystem = FileSystem.get(config);
            }
        } finally {
            fileSystemInitLock.unlock();
        }
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
