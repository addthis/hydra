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

public interface KeyValuePage<K, V> extends KeyValueStore<K, V> {

    /**
     * @return first key of next page or null if last page
     */
    public K getNextFirstKey();

    /**
     * @param key key to test for
     * @return true if the page contains the request key or could uniquely store the key without overlapping other pages
     */
    public boolean mayContainKey(K key);

    /**
     * return a handle that prevents a page from being evicted
     */
    public PagePin pin();

    /**
     * @return true if there are held page pins that are un-released
     */
    public boolean isPinned();

    /**
     * perform internal work that could invalidate page contents (like splitting)
     *
     * @return true if page changed during validation which means a refetch is required
     */
    public boolean reValidate();
}
