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

import java.util.HashMap;

public abstract class LeaseManager<K, V> {

    private HashMap<K, LeaseCounter<K, V>> leases = new HashMap<K, LeaseCounter<K, V>>();
    private HashMap<V, LeaseCounter<K, V>> rev = new HashMap<V, LeaseCounter<K, V>>();

    public abstract V managerLease(K key);

    public abstract void managerRelease(K key, V val);

    public V acquire(K key) {
        synchronized (leases) {
            LeaseCounter<K, V> count = leases.get(key);
            if (count == null) {
                V val = managerLease(key);
                count = new LeaseCounter<K, V>(key, val) {
                    @Override
                    public void release(K key, V val) {
                        rev.remove(val);
                        leases.remove(key);
                        release(key, val);
                    }
                };
                leases.put(key, count);
                rev.put(val, count);
            }
            return count.inc().getValue();
        }
    }

    public void release(V val) {
        synchronized (leases) {
            LeaseCounter<K, V> count = rev.get(val);
            if (count != null) {
                count.dec();
            }
        }
    }
}
