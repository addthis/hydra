package com.addthis.hydra.task.output;

import com.addthis.codec.annotations.FieldConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * OutputWrapperFactory implementation for HDFS systems.
 */
public class HDFSOutputWrapperFactory extends DefaultOutputWrapperFactory {

    private static final Logger log = LoggerFactory.getLogger(HDFSOutputWrapperFactory.class);
    private Lock fileSystemInitLock    = new ReentrantLock();
    private boolean fileSystemInitialized = false;
    private FileSystem fileSystem;

    /**
     * URL for HDFS name node
     */
    @FieldConfig(codable = true)
    private String hdfsURL;

    /**
     * Path to the root directory of the output files.
     */
    @FieldConfig(codable = true)
    private String dir;

    /**
     * When true the file name will include the task id
     */
    @FieldConfig
    private boolean includeTaskId = true;

    /**
     * Opens a write stream for an HDFS output.  Most of the complexity in this
     * method is related to determining the correct file name based on the given
     * <code>target</code> parameter.  If the file already exists and we are appending
     * to an existing file then we will rename that file and open up a new stream which
     * will append data to  that file.  If the file does not exist a new file is created
     * with a .tmp extension.  When the stream is closed the file will be renamed to remove
     * the .tmp extension
     * @param target - the base file name of the target output stream
     * @param outputFlags - output flags setting various options about the output stream
     * @param streamEmitter - the emitter that can convert bundles into the desired byte arrays for output
     * @return a OutputWrapper which can be used to write bytes to the new stream
     * @throws IOException
     */
    @Override
    public OutputWrapper openWriteStream(String target, OutputStreamFlags outputFlags, OutputStreamEmitter streamEmitter) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("[open] " + outputFlags + "target=" + target + " hdfs");
        }
        String rawTarget = target;
        fileSystemInitLock.lock();
        try {
            if (!fileSystemInitialized) {
                File workingDirectory = new File("tmp");
                String path = workingDirectory.getAbsolutePath();
                String[] tokens = path.split("/");
                if (tokens.length > 3 && includeTaskId) {
                    dir = tokens[tokens.length - 4] + "/" + tokens[tokens.length - 3] + "/" + dir;
                } else if (tokens.length > 3) {
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
            if (log.isDebugEnabled()) {
                log.debug("[open.append]" + targetPath.toUri() + "/ renaming to " + targetPathTmp.toUri() + "/" + fileSystem.exists(targetPathTmp));
            }
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

    public void setHdfsURL(String hdfsURL) {
        this.hdfsURL = hdfsURL;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }
}
