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

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.addthis.basis.util.ClosableIterator;

import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueObject;

import com.google.common.collect.Iterators;

public class DataTreeUtil {

    private static Object GLOB_OBJECT = new Object();

    public static Object getGlobObject() { return GLOB_OBJECT; }

    public static final DataTreeNode pathLocateFrom(DataTreeNode input, String[] path) {
        int plen = path.length;
        DataTreeNode current = input;
        for (int i = 0; i < plen; i++) {
            current = current.getNode(path[i]);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    public static final @Nonnull Stream<DataTreeNode> pathLocateFrom(DataTreeNode node, ValueObject[] path) {
        if (path == null || node == null || path.length == 0) {
            return Stream.empty();
        } else {
            return pathLocateFrom(path, 0, Stream.of(node));
        }
    }

    /**
     * Recursively traverse the paths from beginning to end and generate stream of output nodes.
     *
     * @param path      list of specifications of paths to evaluate
     * @param index     current position in the path list
     * @param current   stream of input nodes
     * @return          stream of output nodes
     */
    private static Stream<DataTreeNode> pathLocateFrom(ValueObject[] path, int index,
                                                         Stream<DataTreeNode> current) {
        if (index < path.length) {
            Stream<DataTreeNode> next = current.flatMap((element) -> pathLocateNext(element, path[index]));
            return pathLocateFrom(path, index + 1, next);
        } else {
            return current;
        }
    }

    /**
     * Evaluate the path at the current node and return a stream of results.
     *
     * @param node       current node
     * @param path       specification of path to evaluate
     */
    private static Stream<DataTreeNode> pathLocateNext(DataTreeNode node, ValueObject path) {
        if (path.getObjectType() == ValueObject.TYPE.CUSTOM &&
            path.asNative() == GLOB_OBJECT) {
            ClosableIterator<DataTreeNode> iterator = node.getIterator();
            Stream<DataTreeNode> stream =  StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                    iterator, Spliterator.ORDERED | Spliterator.NONNULL), false);
            stream.onClose(() -> iterator.close());
            return stream;
        } else {
            return Stream.of(node.getNode(ValueUtil.asNativeString(path)));
        }
    }

}
