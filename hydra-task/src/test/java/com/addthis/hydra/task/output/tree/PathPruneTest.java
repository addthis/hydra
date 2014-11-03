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

import java.io.IOException;

import com.addthis.basis.util.ClosableIterator;

import com.addthis.codec.config.Configs;
import com.addthis.hydra.data.tree.DataTreeNode;

import org.joda.time.format.DateTimeFormat;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PathPruneTest {

    @Test
    public void deserialize() throws IOException {
        Configs.decodeObject(PathPrune.class, "nameFormat = YYMMDD");
        Configs.decodeObject(PathElement.class, "prune-ymd = 5 days");
    }

    @Test
    public void formatName() throws IOException {
        PathPrune pathPrune = Configs.decodeObject(PathPrune.class, "nameFormat = YYMMDD");
        DataTreeNode node = Mockito.mock(DataTreeNode.class);
        when(node.getName()).thenReturn("140101");
        long expectedTime = DateTimeFormat.forPattern("YYMMDD").parseMillis("140101");
        assertEquals(expectedTime, pathPrune.getNodeTime(node));
    }

    @Test
    public void deleteFromName() throws IOException {
        DataTreeNode parent = mockParent();
        PathPrune pathPrune = Configs.decodeObject(PathPrune.class, "ttl = 2 days, nameFormat = YYMMDD");
        long now = DateTimeFormat.forPattern("YYMMDD").parseMillis("140105");
        pathPrune.pruneChildren(parent, now);
        verify(parent).deleteNode("140101");
    }

    @Test
    public void keepFromName() throws IOException {
        DataTreeNode parent = mockParent();
        PathPrune pathPrune = Configs.decodeObject(PathPrune.class, "ttl = 20 days, nameFormat = YYMMDD");
        long now = DateTimeFormat.forPattern("YYMMDD").parseMillis("140105");
        pathPrune.pruneChildren(parent, now);
        verify(parent, never()).deleteNode("140101");
    }

    private DataTreeNode mockParent() {
        DataTreeNode child = Mockito.mock(DataTreeNode.class);
        when(child.getName()).thenReturn("140101");

        ClosableIterator<DataTreeNode> childIterator = Mockito.mock(ClosableIterator.class);
        when(childIterator.hasNext()).thenReturn(true).thenReturn(false);
        when(childIterator.next()).thenReturn(child);

        DataTreeNode parent = Mockito.mock(DataTreeNode.class);
        when(parent.getIterator()).thenReturn(childIterator);

        return parent;
    }
}