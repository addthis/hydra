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
package com.addthis.hydra.data.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple Tree that holds only the longest URL branch along with their counts
 */
public class URLTree {

    private TreeObject root;
    private static String PATH_SEPARATOR = "/";

    public URLTree() {
        root = new TreeObject("");
    }

    public void addURLPath(String path, double value) {
        root.addBranch(Arrays.asList(path.split(PATH_SEPARATOR, -1)), value);
    }

    public void add(List<String> data, double value) {
        root.addBranch(data, value);
    }

    public List<TreeObject.TreeValue> getBranches(String sep) {
        return root.getBranches(sep);
    }

    public int size() {
        return root.size();
    }

    public class TreeObject {

        private TreeValue treeValue;
        private Map<String, TreeObject> leaves;

        public TreeObject(String data) {
            treeValue = new TreeValue(data, 0d);
            leaves = new HashMap<String, TreeObject>();
        }

        public TreeValue getTreeValue() {
            return treeValue;
        }

        public int size() {
            return leaves.size();
        }

        public void addBranch(List<String> data, double value) {
            if (data != null) {
                String key = data.get(0);
                TreeObject leaf = leaves.get(key);

                if (leaf == null) {
                    leaf = new TreeObject(key);
                    leaves.put(key, leaf);
                }

                if (data.size() > 1) {
                    leaf.addBranch(data.subList(1, data.size()), value);
                } else {
                    // only the end leaf will contain the value
                    leaf.getTreeValue().updateValue(value);
                }

            }
        }

        public List<TreeValue> getBranches(String sep) {
            List<TreeValue> branches = new ArrayList<TreeValue>();

            if (leaves.size() > 0) {
                for (TreeObject leaf : leaves.values()) {
                    for (TreeValue branch : leaf.getBranches(sep)) {
                        if (treeValue.getData().length() > 0) {
                            branches.add(new TreeValue(new StringBuffer().append(treeValue.getData()).append(sep).append(branch.getData()).toString(), branch.getValue()));
                        } else {
                            branches.add(new TreeValue(new StringBuffer().append(branch.getData()).toString(), branch.getValue()));
                        }
                    }
                }
            } else {
                branches.add(treeValue);
            }

            return branches;
        }

        public class TreeValue {

            private String data;
            private Double value;

            TreeValue(String data, Double value) {
                this.data = data;
                this.value = value;
            }

            public String getData() {
                return data;
            }

            public void setData(String data) {
                this.data = data;
            }

            public Double getValue() {
                return value;
            }


            public void updateValue(Double value) {
                this.value += value;
            }

            public void setValue(Double value) {
                this.value = value;
            }

            public String toString() {
                return data + ":" + value;
            }
        }

    }
}
