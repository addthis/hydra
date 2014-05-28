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
package com.addthis.hydra.data.tree;

import com.addthis.codec.Codec;
import com.addthis.hydra.common.plugins.PluginReader;


/**
 * Data attachments are additional information that can be attached to tree nodes.
 * <p/>
 * <p>Data attachments can be purely informational (such as a {@link DataSum.Config sum}) or they can affect
 * the rest of a tree.  For example a {@link DataLimitTop.Config limit.top} attachment limits how many children
 * a node can have.</p>
 * <p/>
 * <p>When querying a data attachment use either the "${name}={parameters}" or "%{name}={parameters}" syntax.
 * The "$" returns one or more values and the "%" returns a list of nodes.
 * The exact behavior of "$" and "%" are defined on a per data attachment basis.
 * For instance they may not be implemented for each type. The "%" notation may return either
 * fake nodes created during the query or pull real nodes from the tree.</p>
 *
 * @user-reference
 * @hydra-category
 */
@Codec.Set(classMapFactory = TreeDataParameters.CMAP.class)
public abstract class TreeDataParameters implements Codec.Codable {

    static Codec.ClassMap cmap = new Codec.ClassMap() {
        @Override
        public String getClassField() {
            return "type";
        }

        @Override
        public String getCategory() {
            return "data attachment";
        }
    };

    public static class CMAP implements Codec.ClassMapFactory {

        public Codec.ClassMap getClassMap() {
            return cmap;
        }
    }

    public static void registerClass(String key, Class<? extends TreeDataParameters> clazz) {
        cmap.add(key, clazz);
    }

    /** register types */
    static {
        PluginReader.registerPlugin("-treedataparameters.classmap", cmap, TreeDataParameters.class);
        PluginReader.registerPlugin("-treenodedata.classmap", cmap, TreeNodeData.class);
    }

    public abstract TreeNodeData<?> newInstance();
}
