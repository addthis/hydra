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
package com.addthis.hydra.task.source;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.kvp.KVBundle;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This data source <span class="hydra-summary">creates an arbitrary number of empty bundles</span>.
 * <p/>
 * <p>The maxPackets parameter determines how many bundles are created. The default
 * value of maxPackets (-1) indicates that an infinite number of bundles are created.
 *
 * @user-reference
 * @hydra-name empty
 */
public class DataSourceEmpty extends TaskDataSource {

    /**
     * Number of bundles that will be created.
     * Default is -1 which creates an infinite number of bundles.
     */
    @FieldConfig(codable = true)
    private long maxPackets = -1; // go forever

    private AtomicLong packetsCreated = new AtomicLong(0);
    private final KVBundle peek = createBundle();

    private KVBundle createBundle() {
        KVBundle bundle = new KVBundle();
        return bundle;
    }

    @Override
    public void init() {
    }

    @Override
    public Bundle peek() {
        if (maxPackets < 0 || packetsCreated.get() < maxPackets) {
            return peek;
        } else {
            return null;
        }
    }

    @Override
    public Bundle next() {
        if (peek() != null) {
            packetsCreated.getAndIncrement();
            return createBundle();
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void close() {
    }
}
