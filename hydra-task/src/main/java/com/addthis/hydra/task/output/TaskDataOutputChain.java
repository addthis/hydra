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

import java.util.Arrays;
import java.util.List;

import java.nio.file.Path;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.Bundles;
import com.addthis.codec.annotations.FieldConfig;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * This output sink <span class="hydra-summary">writes to multiple output sinks</span>.
 * <p/>
 * <p>Each bundle will be emitted to the first sink, and then the second sink, etc.
 * The {@link #immutableCopy immutableCopy} parameter can be specified if one
 * of the children sinks will modify a bundle, and you do not wish those modifications
 * to appear in the remaining children sinks.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *  output: [
 *      {file {
 *          path:[ "{{DATE_YMD}}", "/","{{PATH_TYPE}}", "/","{{SHARD}}"]
 *          ...
 *      }}
 *      {tree {
 *          root:{const:"ROOT"}
 *          ...
 *      }}
 *  ]
 *  </pre>
 *
 * @user-reference
 */
public class TaskDataOutputChain extends DataOutputTypeList {

    private static final Logger log = LoggerFactory.getLogger(TaskDataOutputChain.class);

    /**
     * Sequence of output sinks. Each bundle is emitted to the first sink,
     * then the second sink, etc.
     */
    @FieldConfig(codable = true, required = true)
    private TaskDataOutput[] outputs;

    /**
     * If true then create copy of the bundle for each output. Default value is true.
     */
    @FieldConfig(codable = true)
    private boolean copy = true;

    /**
     * If true then create a deep copy of a bundle when it is passed to a child
     * output sink. This may be useful when the child sink modifies
     * the bundle. Default value is false.
     */
    @FieldConfig(codable = true)
    private boolean immutableCopy = false;

    @Override
    protected void open() {
        log.warn("[init] beginning init chain");
        for (int i = 0; i < outputs.length; i++) {
            outputs[i].open();
        }
        log.warn("[init] all outputs initialized");
    }

    @Override public void send(Bundle row) throws DataChannelError {
        if (!copy && !immutableCopy) {
            Bundle withPreviousFormat = row;
            for (TaskDataOutput output : outputs) {
                // preserves all mutations, but maintains the typical (admittedly horrible) format behavior
                withPreviousFormat = Bundles.shallowCopyBundle(withPreviousFormat, output.createBundle());
                output.send(withPreviousFormat);
            }
        } else if (immutableCopy) {
            for (TaskDataOutput output : outputs) {
                output.send(Bundles.deepCopyBundle(row, output.createBundle()));
            }
        } else {
            for (TaskDataOutput output : outputs) {
                output.send(Bundles.shallowCopyBundle(row, output.createBundle()));
            }
        }
    }

    @Override public void send(List<Bundle> bundles) {
        if (bundles != null && !bundles.isEmpty()) {
            for (Bundle bundle : bundles) {
                send(bundle);
            }
        }
    }

    @Override public void sendComplete() {
        log.warn("[sendComplete] forwarding completion signal to all outputs");
        for (TaskDataOutput output : outputs) {
            output.sendComplete();
        }
        log.warn("[sendComplete] forwarding complete");
    }

    @Override public void sourceError(Throwable er) {
        log.warn("[sourceError] forwarding to all outputs" + er);
        for (TaskDataOutput output : outputs) {
            output.sourceError(er);
        }
        log.warn("[sourceError] forwarding complete");
    }


    @Nonnull @Override public ImmutableList<Path> writableRootPaths() {
        return ImmutableList.copyOf(
                Arrays.stream(outputs).flatMap(
                        output -> output.writableRootPaths().stream()).iterator());
    }

}
