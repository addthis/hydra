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
package com.addthis.hydra.data.tree.concurrent;

import com.addthis.basis.concurrentlinkedhashmap.EvictionMediator;

import com.addthis.hydra.data.tree.CacheKey;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.IPageDB;

class CacheMediator implements EvictionMediator<CacheKey, ConcurrentTreeNode> {

    private final IPageDB<DBKey, ConcurrentTreeNode> source;

    public CacheMediator(IPageDB<DBKey, ConcurrentTreeNode> source) {
        this.source = source;
    }

    @Override
    public boolean onEviction(CacheKey key, ConcurrentTreeNode value) {
        boolean evict = value.trySetEviction();
        if (evict) {
            try {
                if (!value.isDeleted() && value.isChanged()) {
                    source.put(key.dbkey(), value);
                }
            } finally {
                value.evictionComplete();
            }
        }
        return evict;
    }
}
