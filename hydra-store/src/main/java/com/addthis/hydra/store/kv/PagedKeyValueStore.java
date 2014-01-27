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
package com.addthis.hydra.store.kv;

import com.addthis.hydra.store.db.CloseOperation;

/**
 * A KV store of KV stores where the contained KV stores are called pages.
 * Pages are ordered using the same mechanism as keys.  Keys can appear
 * in at most one page at a time and pages may not overlap.
 *
 * @param <K>
 * @param <V>
 */
public interface PagedKeyValueStore<K, V> extends KeyValueStore<K, V> {

    public void setMaxPages(int maxPages);

    public void setMaxPageSize(int maxPageSize);

    public void setMaxPageMem(long maxPageMem);

    public void setMaxTotalMem(long maxTotalMem);

    public void setMemEstimateInterval(int interval);

    public void close();

    /**
     * Close the store.
     *
     * @param cleanLog if true then wait for the BerkeleyDB clean thread to finish.
     * @param operation optionally test or repair the berkeleyDB.
     * @return status code. A status code of 0 indicates success.
     **/
    public int close(boolean cleanLog, CloseOperation operation);
}
