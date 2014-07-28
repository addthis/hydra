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
package com.addthis.hydra.data.tree.prop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;
import com.addthis.hydra.data.tree.TreeNodeDataDeferredOperation;
import com.addthis.hydra.data.util.KeyHistogram;

/**
 * keep a histogram of the counts of nodes with bucketed # of hits
 */
public class DataHistogram extends TreeNodeData<DataHistogram.Config> implements Codable {

    /**
     * This data attachment <span class="hydra-summary">keeps a histogram of the counts of child nodes</span>.
     * <p/>
     * <p>As many buckets as needed are created for this histogram. The range of the buckets
     * is determined by the {@link #scale} parameter. Each bucket represents an interval
     * from <i>scale</i> <sup><i>i</i></sup> to <i>scale</i> <sup><i>i+1</i></sup> for an
     * arbitrary value of <i>i</i>.</p>
     * <p/>
     * <p>Job Configuration Example:</p>
     * <pre>
     *   {type : "const", value : "pageUrlLengths"},
     *   {type : "const", value : "by_date"},
     *   {type : "value", key : "DATE_YMD", data: {
     *     // powers of 10 histogram
     *     page_url_histo : {type : "histo", scale : 10},
     *   }},
     *   {type : "value", key : "PAGE_URL_LENGTH"}</pre>
     *
     * <p><b>Query Path Directives</b>
     *
     * <p>${attachment} returns the string representation of the histogram.
     *
     * <p>%{attachment}=tiers create a virtual node for each bucket in the histogram.
     *
     * <p>Query Path Examples:</p>
     * <pre>
     *     /pageUrlLengths/by_date/130707$+page_url_histo
     *     /pageUrlLengths/by_date/130707/+%page_url_histo=tiers:+count
     * </pre>
     *
     * @user-reference
     * @hydra-name histo
     */
    public static final class Config extends TreeDataParameters<DataHistogram> {

        /**
         * Base for the interval of the exponentially sized buckets.
         * A positive integer greater than one.
         * This field is required.
         */
        @FieldConfig(codable = true)
        private int scale;

        @Override
        public DataHistogram newInstance() {
            DataHistogram dt = new DataHistogram();
            dt.histo = new KeyHistogram().setScale(scale).init();
            return dt;
        }
    }

    @FieldConfig(codable = true)
    private KeyHistogram histo;

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode childNode, Config conf) {
        return false;
    }

    @Override
    public boolean updateParentData(DataTreeNodeUpdater state, DataTreeNode parentNode,
            DataTreeNode childNode,
            List<TreeNodeDataDeferredOperation> deferredOps) {
        return histo.incrementTo(childNode.getCounter());
    }

    @Override
    public ValueObject getValue(String key) {
        return ValueFactory.create(histo.getHistogram().toString());
    }

    @Override
    public List<String> getNodeTypes() {
        return Arrays.asList(new String[]{"tiers"});
    }

    @Override
    public List<DataTreeNode> getNodes(DataTreeNode parent, String key) {
        if (key == null) {
            return null;
        }
        if (key.equals("tiers")) {
            Map<Long, Long> map = histo.getHistogram();
            ArrayList<DataTreeNode> tiers = new ArrayList<>(map.size());
            for (Entry<Long, Long> e : histo.getSortedHistogram().entrySet()) {
                tiers.add(new VirtualTreeNode(e.getKey().toString(), e.getValue()));
            }
            return tiers;
        }
        return null;
    }
}
