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
package com.addthis.hydra.task.output.tree;

import java.io.File;
import java.io.IOException;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.kvp.KVBundle;
import com.addthis.codec.codables.Codable;
import com.addthis.hydra.data.tree.DataTree;
import com.addthis.hydra.data.tree.concurrent.ConcurrentTree;
import com.addthis.hydra.task.output.DataOutputTypeList;
import com.addthis.hydra.task.run.TaskRunConfig;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This output <span class="hydra-summary">transforms bundle streams into trees for statistical analysis and data queries</span>
 * <p/>
 * <p>A tree is defined by one or more paths. A path is a reusable set of connected tree nodes. One of these
 * paths is designated as the root path, from which the rest of the tree is constructed.</p>
 * <p/>
 * <p>A tree may optionally specify a feature set. The feature set is a set of character strings.
 * Path elements that do not match against the feature set will not be included in the tree.
 * If a path element specifies the {@link PathElement#feature feature} parameter then the path
 * element is excluded if it specifies a feature that is not in the feature set of the tree.
 * If a path element specifies the {@link PathElement#featureOff featureOff} parameter then the path
 * element is excluded if it specifies a feature that is in the feature set of the tree.
 * <p/>
 * <p>Example:</p>
 * <pre>output : {
 *   type : "tree",
 *   live : true,
 *   timeField : {
 *     field : "TIME",
 *     format : "native",
 *   },
 *   root : { path : "ROOT" },
 * <p/>
 *   paths : {
 *     "ROOT" : [
 *       {type : "const", value : "date"},
 *       {type : "value", key : "DATE_YMD"},
 *       {type : "value", key : "DATE_HH"},
 *     ],
 *   },
 * },</pre>
 *
 * @user-reference
 * @hydra-name tree
 */
public final class TreeMapper extends DataOutputTypeList implements Codable {

    private static final Logger log = LoggerFactory.getLogger(TreeMapper.class);

    /**
     * Definition of the tree structure.
     * Consists of a mapping from a name to one or more path elements.
     * One of these path elements will serve as the root of the tree.
     */
    @JsonProperty private Map<String, PathElement[]> paths;

    /**
     * Optional path that is processed once at the beginning of execution. The input to this path is an empty bundle.
     */
    @JsonProperty private PathElement[] pre;

    /** Path that will serve as the root of the output tree. */
    @JsonProperty private PathElement[] root;

    /** Optional path that is processed once at the end of execution. The input to this path is an empty bundle. */
    @JsonProperty private PathElement[] post;

    /**
     * Optional sample rate for applying the {@link #post post} paths. If greater than one than apply
     * once every N runs. Default is one.
     */
    @JsonProperty private int postRate;

    /** One or more queries that are executed after the tree has been constructed. */
    @JsonProperty private PathOutput[] outputs;

    /** Set of strings that enumerate the features to process. */
    @JsonProperty private HashSet<String> features;

    @JsonProperty private TaskRunConfig config;

    private final AtomicLong processNodes = new AtomicLong(0);

    private DataTree tree;

    @Override
    public void open() {
        try {
            resolvePathElements();

            log.info("[init] target={} job={}", root, this.config.jobId);

            Path treePath = Paths.get(config.dir, "data");
            tree = new ConcurrentTree(Files.initDirectory(treePath.toFile()));

            if (pre != null) {
                log.info("pre-chain: {}", pre);
                processBundle(new KVBundle(), pre);
            }
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    private void resolvePathElements() throws Exception {
        // index paths and intern path element keys
        if (paths != null) {
            for (PathElement[] pe : paths.values()) {
                for (PathElement p : pe) {
                    p.resolve(this);
                }
            }
        }
        if (root != null) {
            for (PathElement p : root) {
                p.resolve(this);
            }
        } else if ((paths != null) && !paths.isEmpty()) {
            root = paths.values().iterator().next();
        }
        if (pre != null) {
            for (PathElement p : pre) {
                p.resolve(this);
            }
        }
        if (post != null) {
            for (PathElement p : post) {
                p.resolve(this);
            }
        }
        if (outputs != null) {
            for (PathOutput out : outputs) {
                out.resolve(this);
            }
        }
    }

    public PathElement[] getPath(String path) {
        return paths.get(path);
    }

    public boolean isFeatureEnabled(String feature) {
        return features.contains(feature);
    }

    // ------------------------- PROCESSING ENGINE -------------------------

    @Override
    public void send(Bundle bundle) {
        processBundle(bundle, root);
    }

    public void processBundle(Bundle bundle, PathElement[] path) {
        TreeMapState ps = new TreeMapState(this, tree, path, bundle);
        processNodes.addAndGet(ps.touched());
    }

    @Override
    public void sourceError(Throwable cause) {
        log.error("source error reported; closing output in response", cause);
        sendComplete();
    }

    @Override
    public void sendComplete() {
        try {
            if (post != null) {
                maybeDoPost();
            }
            if (outputs != null) {
                for (PathOutput output : outputs) {
                    log.warn("output: {}", output);
                    output.exec(tree);
                }
            }
            // close storage
            log.info("[close] closing tree storage");
            tree.close();
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    private void maybeDoPost() throws IOException {
        int sample = 0;
        boolean doPost = false;
        if (postRate > 1) {
            File sampleFile = new File("post.sample");
            if (sampleFile.canRead()) {
                try {
                    sample = Integer.parseInt(Bytes.toString(Files.read(sampleFile)));
                    sample = (sample + 1) % postRate;
                } catch (NumberFormatException ignored) {
                }
            }
            doPost = sample == 0;
            Files.write(sampleFile, Bytes.toBytes(Integer.toString(sample)), false);
        } else {
            doPost = true;
        }
        if (doPost) {
            log.warn("post-chain: {}", post);
            processBundle(new KVBundle(), post);
        } else {
            log.warn("skipping post-chain: {}. Sample rate is {} out of {}", post, sample, postRate);
        }
    }
}
