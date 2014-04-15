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
package com.addthis.hydra.task.treestats;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.addthis.codec.Codec;
import com.addthis.hydra.data.tree.ConcurrentTree;
import com.addthis.hydra.data.tree.ReadTree;
import com.addthis.hydra.data.tree.TreeStatistics;
import com.addthis.hydra.store.db.SettingsDB;
import com.addthis.hydra.store.db.SettingsJE;
import com.addthis.hydra.task.run.TaskRunConfig;
import com.addthis.hydra.task.run.TaskRunnable;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * This Hydra job is <span class="hydra-summary">a diagnostic utility for map jobs</span>.
 * <p/>
 * <p>This job will traverse over the tree output of another job and generate disk
 * usage statistics of the input tree. {@link #input input} is a required parameter
 * which specifies the job ID of the input job. A treestats job is designed to run
 * on a single node. If you allocate multiple nodes for the job only the first node
 * will perform a traversal.</p>
 * <p/>
 * <p>The treestats job has the requirement that it must run on a minion that includes
 * a tree from the input job. You will likely need to force this job to run on a specific
 * minion. This can be specified by the notation '// hosts: "hostUUID"' at the top
 * of the job configuration (see example).</p>
 * <p/>
 * <p>The default job configuration is to iterate over a representative sample of
 * the input tree. At any level of the tree if the level has fewer than
 * {@link #children children} nodes then all the nodes of that level are visited.
 * If the level has more than {@link #children children} then sampling is performed.
 * The sampling will select one page out of every {@link #sampleRate sampleRate} pages.
 * Note that sampling is performed on a per-page basis and not a per-node basis.
 * (A page is a collection of nodes stored together on disk). Setting the sampleRate to
 * one will disable the sampling behavior.</p>
 * <p/>
 * <p>In the output tree the node property "count" has been used to store disk usage
 * statistics in bytes. For each node in the input tree that was sampled, there will
 * be a corresponding node in the output tree with three child nodes: "node", "children",
 * and "histograms". "node" contains information pertaining to the individual nodes.
 * "children" contains information regarding the children nodes from the input job.
 * And "histograms" contains histogram information that summarizes all the children
 * nodes. Within the "histograms" node there is a child "attachments" that
 * contains information for each data attachment that appears in the child nodes.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 * //hosts: "eee19db4-f5f9-4420-9c36-64292825116b"
 * {
 *   type : "treestats",
 *   input : "abcde-1234-helloworld"
 * }</pre>
 *
 * @user-reference
 * @hydra-name treestats
 */
public class TreeStatisticsJob extends TaskRunnable implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TreeStatisticsJob.class);

    /**
     * Job ID of the input job. This field is required.
     */
    @Codec.Set(codable = true, required = true)
    private String input;

    /**
     * Sample rate for the input database.
     * A sample rate of N means that only 1 page
     * is read for every N leaf pages in the input tree.
     * Default is 100.
     */
    @Codec.Set(codable = true)
    private int sampleRate = 100;

    /**
     * Minimum children limit for each level of the tree.
     * A child limit of N means that any level that contains
     * more than N child nodes will not iterate over all
     * of the nodes but instead perform a sampling of the nodes.
     * Default is 100.
     */
    @Codec.Set(codable = true)
    private int children = 100;

    /**
     * Change this setting if you know what you're doing.
     * Default is "500MB".
     */
    @Codec.Set(codable = true)
    private String jeCacheSize = "500MB";

    private Thread thread;

    private TaskRunConfig config;

    private AtomicBoolean terminating = new AtomicBoolean(false);

    @Override
    public void run() {
        ReadTree inputTree = null;
        ConcurrentTree outputTree = null;

        boolean failure = false;

        if (config.node > 0) {
            return;
        }

        try {
            String cacheSize = System.getProperty(SettingsJE.JE_CACHE_SIZE);

            if (!cacheSize.endsWith("%")) {
                long minCacheSize = SettingsDB.parseNumber(jeCacheSize);
                long cacheSizeValue = (cacheSize == null) ? 0 : SettingsDB.parseNumber(cacheSize);
                if (cacheSizeValue < minCacheSize) {
                    System.setProperty(SettingsJE.JE_CACHE_SIZE, Long.toString(minCacheSize));
                }
            }

            File inputFile = getInputDirectory();
            Path outputPath = Paths.get(config.dir, "data");
            File outputFile = outputPath.toFile();

            inputTree = new ReadTree(inputFile, true);

            outputTree = new ConcurrentTree(
                    com.addthis.basis.util.Files.initDirectory(outputFile));

            TreeStatistics statistics = new TreeStatistics(inputTree, outputTree,
                    sampleRate, children, terminating);

            statistics.generateStatistics();


        } catch (Exception ex) {
            System.err.println(ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            System.err.println(sw.toString());
            failure = true;
        } finally {
            if (outputTree != null) {
                outputTree.close();
            }

            if (inputTree != null) {
                inputTree.close();
            }

            if (failure) {
                /**
                 * We need to exit in a separate thread. We setup a
                 * shutdown hook that invokes join on this thread and
                 * that was a bad idea.
                 * An alternative to System.exit would be
                 * ideal but that would require some more invasive
                 * changes.
                 **/
                new Thread(new Runnable() {
                    public void run() {
                        System.exit(1);
                    }
                }, "TreeStatisticsExit").start();
            }
        }
    }

    private File getInputDirectory() throws IOException {
        Path workingDirectory = Paths.get(config.dir).toAbsolutePath();
        int workDirCount = workingDirectory.getNameCount();

        if (workDirCount < 4) {

            String msg =
                    "Could not find the path other jobs on this host from the current path " +
                    workingDirectory;
            generateError(msg);
        }

        Path parent = eliminateChildren(workingDirectory, 4);
        Path otherJob = parent.resolve(input);

        if (!Files.exists(otherJob)) {
            String msg =
                    "Could not find a directory at path " +
                    otherJob.toString();
            generateError(msg);
        }


        DirectoryStream<Path> children = Files.newDirectoryStream(otherJob, new DirFilter());

        Iterator<Path> iterator = children.iterator();

        if (!iterator.hasNext()) {
            String msg =
                    "Could not find any task directories at path " +
                    otherJob.toString();
            generateError(msg);
        }

        Path select = iterator.next();

        log.warn("Selected path " + select.toAbsolutePath());

        Path goldDir = select.resolve("gold");

        if (!Files.exists(goldDir)) {
            String msg =
                    "Could not the gold directory at path " +
                    goldDir.toAbsolutePath();
            generateError(msg);
        }

        Path dataDir = goldDir.resolve("data");

        if (!Files.exists(dataDir)) {
            String msg =
                    "Could not the data directory at path " +
                    dataDir.toAbsolutePath();
            generateError(msg);
        }

        return dataDir.toFile();
    }

    /**
     * I would have preferred to use {@link Path#subpath(int, int)}
     * but subpath always returns a relative path and I want
     * to maintain the absolute path. The output is garbage
     * if you invoke {@link java.nio.file.Path#toAbsolutePath()}
     * on an absolute path that is masquerading as a relative path.
     *
     * @param input
     * @param count
     * @return
     */
    private static Path eliminateChildren(Path input, int count) {
        Path output = input;
        for (int i = 0; i < count; i++) {
            output = output.getParent();
        }
        return output;
    }

    private static class DirFilter implements DirectoryStream.Filter<Path> {

        @Override
        public boolean accept(Path entry) throws IOException {
            return Files.isDirectory(entry);
        }
    }

    private void generateError(String msg) {
        log.warn(msg);
        throw new IllegalStateException(msg);
    }

    @Override
    public void init(TaskRunConfig config) {
        this.config = config;
    }

    @Override
    public void exec() {
        thread = new Thread(this);
        thread.setName("TreeStatisticsJob main thread");
        thread.start();
        log.warn("exec " + config.jobId);
    }

    @Override
    public void terminate() {
        if (terminating.compareAndSet(false, true)) {
            log.warn("terminate " + config.jobId);
        }
    }

    @Override
    public void waitExit() {
        try {
            thread.join();
            log.warn("exit " + config.jobId);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
