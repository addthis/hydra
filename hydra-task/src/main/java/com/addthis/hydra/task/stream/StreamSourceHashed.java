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
package com.addthis.hydra.task.stream;

import com.addthis.hydra.common.hash.PluggableHashFunction;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public class StreamSourceHashed implements StreamFileSource {

    private static final Logger log = LoggerFactory.getLogger(StreamSourceHashed.class);

    private final StreamFileSource wrap;
    private final Integer[] shards;
    private final int mod;
    private final boolean legacy;

    public StreamSourceHashed(StreamFileSource wrap, Integer[] shards, int mod) {
        this(wrap, shards, mod, false);
    }

    public StreamSourceHashed(StreamFileSource wrap, Integer shards[], int mod, boolean legacy) {
        this.wrap = wrap;
        this.shards = shards;
        this.mod = mod;
        this.legacy = legacy;
    }

    @Override
    public StreamFile nextSource() {
        StreamFile next = null;
        while ((next = wrap.nextSource()) != null) {
            String path = next.getPath();
            if (legacy) {
                if (path.charAt(0) == '/') {
                    path = path.substring(1);
                }
                path = replaceGoldWithLiveDotDotGold(path);
            }
            int hash = Math.abs(PluggableHashFunction.hash(path) % mod);
            for (Integer i : shards) {
                if (hash == i) {
                    log.debug("hash match {}", next);
                    return next;
                }
            }
            continue;
        }
        return null;
    }

    public static String replaceGoldWithLiveDotDotGold(String input) {
        // Function name of the year!
        if (input != null && input.contains("/gold/")) {
            input = input.replace("/gold/", "/live/../gold/");
        }
        return input;
    }
}
