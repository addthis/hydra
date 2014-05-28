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

import java.util.Iterator;
import java.util.Map;

import com.addthis.hydra.store.db.DBKey;

/**
 * Does not interact with node cache in tree
 */
final class NameSettingIteratorWrapper implements Iterator<ReadNode> {

    private final Iterator<Map.Entry<DBKey, ReadTreeNode>> wrappedIterator;

    NameSettingIteratorWrapper(Iterator<Map.Entry<DBKey, ReadTreeNode>> range) {
        this.wrappedIterator = range;
    }

    @Override
    public boolean hasNext() {
        return wrappedIterator.hasNext();
    }

    @Override
    public ReadNode next() {
        Map.Entry<DBKey, ReadTreeNode> next = wrappedIterator.next();
        ReadTreeNode node = next.getValue();
        node.initName(next.getKey().rawKey().toString());
        return node;
    }

    @Override
    public void remove() {
        wrappedIterator.remove();
    }
}
