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

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.LinkedHashMultimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Objects.toStringHelper;

public class StreamSourceByPaths<E extends StreamFile> implements StreamSourceGrouped<E> {

    private static final Logger log = LoggerFactory.getLogger(StreamSourceByPaths.class);

    private final LinkedHashMultimap<String, E> cache = LinkedHashMultimap.create();

    /**
     * Fills the cache and cacheMap with paths and mappings of those paths to mesh host options
     * The cache is an ordered list which reflects the ordering specified by the sort offset.
     */
    public void fillCache(String timeToLoad, Collection<E> streamSources) {
        log.debug("filling cache for date {}", timeToLoad);

        long start = System.currentTimeMillis();
        for (E streamSource : streamSources) {
            log.debug("cache added {}", streamSource);
            cache.put(streamSource.getPath(), streamSource);
        }
        long end = System.currentTimeMillis();
        log.info(toStringHelper("File reference cache fill")
                .add("date", timeToLoad)
                .add("files-found", streamSources.size())
                .add("after-filtering", cache.size())
//                .add("fills-left", dates.size())
                .add("fill-time", (end - start))
                .toString());
    }

    public boolean isEmpty() {
        return cache.isEmpty();
    }

    public String peekKey() {
        return cache.keySet().iterator().next();
    }

    @Override
    public Set<E> nextGroup() {
        String nextKey = peekKey();
        return cache.removeAll(nextKey);
    }
}
