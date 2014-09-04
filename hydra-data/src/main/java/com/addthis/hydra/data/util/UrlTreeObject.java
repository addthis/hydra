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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class UrlTreeObject {

    private TreeValue treeValue;
    private Map<String, UrlTreeObject> leaves;
    private boolean hasTrailingSep;

    public UrlTreeObject(String data) {
        treeValue = new TreeValue(data, 0d);
        leaves = new HashMap<>();
    }

    public TreeValue getTreeValue() {
        return treeValue;
    }

    public int size() {
        return leaves.size();
    }

    public boolean hasTrailingSlash() {
        return hasTrailingSep;
    }

    public void setHasTrailingSep(boolean hasTrailingSep) {
        this.hasTrailingSep = hasTrailingSep;
    }

    public void addBranch(List<String> data, double value, boolean hashTrailingSep) {
        if (data != null) {
            String key = data.get(0);
            UrlTreeObject leaf = leaves.get(key);

            if (leaf == null) {
                leaf = new UrlTreeObject(key);
                leaves.put(key, leaf);
            }

            if (data.size() > 1) {
                List<String> subList = data.subList(1, data.size());
                leaf.addBranch(subList, value, hashTrailingSep);
            } else {
                // only the end leaf will contain the value
                leaf.getTreeValue().updateValue(value);
                leaf.setHasTrailingSep(hashTrailingSep);
            }

        }
    }

    public List<TreeValue> getBranches(String sep) {
        List<TreeValue> branches = new ArrayList<>();

        if (leaves.size() > 0) {
            for (UrlTreeObject leaf : leaves.values()) {
                for (TreeValue branch : leaf.getBranches(sep)) {
                    if (treeValue.getData().length() > 0) {
                        branches.add(new TreeValue(new StringBuffer().append(treeValue.getData())
                                                                     .append(sep)
                                                                     .append(branch.getData())
                                                                     .toString(), branch.getValue()));
                    } else {
                        branches.add(new TreeValue(new StringBuffer().append(branch.getData()).toString(),
                                                   branch.getValue()));
                    }
                }
            }
        } else {
            if (hasTrailingSep) {
                branches.add(new TreeValue(treeValue.getData() + (hasTrailingSep ? sep : ""), treeValue.getValue()));
            } else {
                branches.add(treeValue);
            }
        }

        return branches;
    }

    public static class TreeValue {

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

        @Override public boolean equals(Object obj) {
            if (!(obj instanceof TreeValue)) {
                return false;
            }
            TreeValue other = (TreeValue) obj;
            return Objects.equals(this.data, other.data) && Objects.equals(this.value, other.value);
        }

        @Override public int hashCode() {
            return Objects.hash(data, value);
        }
    }
}
