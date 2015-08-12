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

import java.io.File;

import com.addthis.hydra.data.tree.DataTree;
import com.addthis.hydra.data.tree.TreeCommonParameters;
import com.addthis.hydra.data.tree.nonconcurrent.NonConcurrentTree;
import com.addthis.hydra.store.nonconcurrent.NonConcurrentPage;
import com.addthis.hydra.store.skiplist.ConcurrentPage;
import com.addthis.hydra.store.common.PageFactory;

public class Builder<T extends DataTree> {

    // Required parameters
    protected final File root;
    private final boolean concurrent;

    // Optional parameters - initialized to default values;
    protected int numDeletionThreads = ConcurrentTree.defaultNumDeletionThreads;
    protected int cleanQSize = TreeCommonParameters.cleanQMax;
    protected int maxCache = TreeCommonParameters.maxCacheSize;
    protected int maxPageSize = TreeCommonParameters.maxPageSize;
    protected PageFactory concurrentPageFactory = ConcurrentPage.ConcurrentPageFactory.singleton;
    protected PageFactory nonConcurrentPageFactory = NonConcurrentPage.NonConcurrentPageFactory.singleton;
    protected PageFactory pageFactory;

    public Builder(File root) {
        this(root, true);
    }

    public Builder(File root, boolean concurrent) {
        this.root = root;
        this.concurrent = concurrent;

    }

    public Builder numDeletionThreads(int val) {
        numDeletionThreads = val;
        return this;
    }

    public Builder nodeCacheSize(int val) {
        cleanQSize = val;
        return this;
    }

    public Builder maxCacheSize(int val) {
        maxCache = val;
        return this;
    }

    public Builder maxPageSize(int val) {
        maxPageSize = val;
        return this;
    }

    public Builder pageFactory(PageFactory factory) {
        pageFactory = factory;
        return this;
    }

    public DataTree build() throws Exception {
        if (concurrent) {
            pageFactory = concurrentPageFactory;
            return new ConcurrentTree(root, numDeletionThreads, cleanQSize,
                    maxCache, maxPageSize, pageFactory);
        } else {
            pageFactory = nonConcurrentPageFactory;
            return new NonConcurrentTree(root, maxCache, maxPageSize, pageFactory);
        }
    }
}
