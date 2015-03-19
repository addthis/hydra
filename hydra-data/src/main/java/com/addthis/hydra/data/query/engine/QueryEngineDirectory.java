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
package com.addthis.hydra.data.query.engine;

import java.io.File;
import java.io.IOException;

import java.util.concurrent.TimeUnit;

import com.addthis.hydra.data.tree.DataTree;
import com.addthis.hydra.data.tree.ReadTree;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends QueryEngine and keeps track of the directory it is reading data from
 * <p/>
 * Has a convenience function specific to our directory structures that compares
 * paths to determine if there is new data available.
 */
public class QueryEngineDirectory extends QueryEngine {

    private static final Logger log = LoggerFactory.getLogger(QueryEngineDirectory.class);

    /**
     * metric to track the number of open engines
     */
    private static final Counter currentlyOpenEngines = Metrics.newCounter(QueryEngineDirectory.class, "currentlyOpenEngines");

    /**
     * metric to track the number of engines opened. Should be an aggregate of new and refresh'd
     */
    protected static final Meter engineCreations = Metrics.newMeter(QueryEngineCache.class, "engineCreations",
            "engineCreations", TimeUnit.MINUTES);

    private final String dir;

    public QueryEngineDirectory(DataTree tree, String dir) {
        super(tree);
        this.dir = dir;
        currentlyOpenEngines.inc();
        engineCreations.mark(); //Metric for total trees/engines initialized
    }

    public void loadAllFrom(QueryEngineDirectory other) {
        ((ReadTree) tree).warmCacheFrom(((ReadTree) other.getTree()).getCacheIterable());
    }

    @Override
    public void close() throws IOException {
        super.close();
        currentlyOpenEngines.dec();
    }

    public boolean isOlder(String dir) {
        try {
            final String currentCanonical = getDirectory();
            final String newCanonical = new File(dir).getCanonicalPath();
            return currentCanonical.compareTo(newCanonical) < 0;
        } catch (Exception e) {
            log.warn("Exception getting query engine path comparison", e);
        }
        return false;
    }

    public String getDirectory() {
        return dir;
    }

    @Override
    public String toString() {
        return "[QueryEngineDirectory:" + dir + ":" + super.toString() + "]";
    }
}
