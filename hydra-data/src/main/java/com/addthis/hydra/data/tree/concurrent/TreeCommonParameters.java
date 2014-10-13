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

import com.addthis.basis.util.Parameter;

public class TreeCommonParameters {

    static int cleanQMax = Math.max(1, Parameter.intValue("hydra.tree.cleanqmax", 100));
    // max number of pages allowed to reside in memory
    static int maxCacheSize = Parameter.intValue("hydra.tree.cache.maxSize", 0);
    // max mem for all pages in memory
    static long maxCacheMem = Parameter.longValue("hydra.tree.cache.maxMem", 0);
    // max number of keys in a single page
    static int maxPageSize = Parameter.intValue("hydra.tree.page.maxSize", 0);
    // max memory for a given page
    static int maxPageMem = Parameter.intValue("hydra.tree.page.maxMem", 0);
    static int memSample = Parameter.intValue("hydra.tree.mem.sample", 0);
    static int meterLogging = Parameter.intValue("hydra.tree.meterlog", 0);
    static final int meterLogLines = Parameter.intValue("hydra.tree.loglines", 100000);
    static int cacheShards = Parameter.intValue("hydra.tree.shards", Runtime.getRuntime().availableProcessors() * 8);
    static long trashInterval = Parameter.longValue("hydra.tree.trash.interval", 0);
    static long trashMaxTime = Parameter.intValue("hydra.tree.trash.maxtime", 0);

    public static void setDefaultMaxCacheSize(int size) {
        maxCacheSize = size;
    }

    public static void setDefaultMaxPageSize(int size) {
        maxPageSize = size;
    }

    public static void setDefaultMaxCacheMem(long mem) {
        maxCacheMem = mem;
    }

    public static void setDefaultMaxPageMem(int mem) {
        maxPageMem = mem;
    }

    public static void setDefaultCleanQueueSize(int size) {
        cleanQMax = size;
    }

    public static void setDefaultMemSample(int sample) {
        memSample = sample;
    }

    public static void setDefaultMeterLogging(int meter) {
        meterLogging = meter;
    }

    public static void setDefaultCacheShards(int shards) {
        cacheShards = shards;
    }

    public static void setDefaultTrashInterval(long trashTime) {
        trashInterval = trashTime;
    }

    public static void setDefaultTrashTimeLimit(long trashTimeLimit) {
        trashMaxTime = trashTimeLimit;
    }

}
