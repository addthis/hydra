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
package com.addthis.hydra.query.util;

public abstract class LeaseCounter<K, V> {

    private K key;
    private V val;
    private int ref;

    public LeaseCounter(K key, V val) {
        this.key = key;
        this.val = val;
    }

    public LeaseCounter<K, V> inc() {
        ref++;
        return this;
    }

    public LeaseCounter<K, V> dec() {
        if (--ref == 0) {
            release(key, val);
        }
        return this;
    }

    public int getCount() {
        return ref;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return val;
    }

    /**
     * override for callback
     */
    public abstract void release(K key, V val);
}
