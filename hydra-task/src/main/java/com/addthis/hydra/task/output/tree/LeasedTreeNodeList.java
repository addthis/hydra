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

import java.util.ArrayList;
import java.util.Collections;
import java.util.function.Consumer;

import com.addthis.hydra.data.tree.DataTreeNode;

import io.netty.util.ResourceLeak;
import io.netty.util.ResourceLeakDetector;

public class LeasedTreeNodeList {

    private static final ResourceLeakDetector<ReadOnceList<DataTreeNode>> leakDetector = new ResourceLeakDetector<>(ReadOnceList.class);

    private static final Consumer<DataTreeNode> releaser = (x) -> x.release();

    // TODO replace with proper singleton such as Collections.EMPTY_LIST or ImmutableList.of()
    private static final ReadOnceList<DataTreeNode> empty = new ReadOnceListSimple<>(releaser,
                                                                                     Collections.unmodifiableList(new ArrayList<>()));

    public static ReadOnceList<DataTreeNode> create() {
        ReadOnceList<DataTreeNode> list = new ReadOnceListSimple<>(releaser);
        ResourceLeak leak = leakDetector.open(list);
        if (leak != null) {
            return new ReadOnceListLeakDetection<>(leak, list);
        } else {
            return list;
        }
    }

    public static ReadOnceList<DataTreeNode> create(int capacity) {
        ReadOnceList<DataTreeNode> list = new ReadOnceListSimple<>(releaser, capacity);
        ResourceLeak leak = leakDetector.open(list);
        if (leak != null) {
            return new ReadOnceListLeakDetection<>(leak, list);
        } else {
            return list;
        }
    }


    public static ReadOnceList<DataTreeNode> unmodifiableList() {
        return empty;
    }

}
