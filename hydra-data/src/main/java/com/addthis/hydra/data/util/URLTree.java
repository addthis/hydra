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

import java.util.Arrays;
import java.util.List;

/**
 * Simple Tree that holds only the longest URL branch along with their counts
 */
public class URLTree {

    private UrlTreeObject root;
    private static final String PATH_SEPARATOR = "/";

    public URLTree() {
        root = new UrlTreeObject("");
    }

    public void addURLPath(String path, double value) {
        root.addBranch(Arrays.asList(path.split(PATH_SEPARATOR)), value, path.endsWith(PATH_SEPARATOR));
    }

    public void add(List<String> data, double value) {
        root.addBranch(data, value, false);
    }

    public List<UrlTreeObject.TreeValue> getBranches(String sep) {
        return root.getBranches(sep);
    }

    public int size() {
        return root.size();
    }
}
