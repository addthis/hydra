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
package com.addthis.hydra.store.common;


public enum ExternalMode {
    // The copy of the page in external storage is the most up-to-date copy.
    DISK_MEMORY_IDENTICAL,

    // The copy of the page in memory is more recent than the copy in external storage.
    DISK_MEMORY_DIRTY,

    // The page has been removed from the page cache. A copy exists on disk.
    MEMORY_EVICTED,

    // The page has been removed from both the page cache and the external storage.
    DELETED;

    public boolean isTransient() {
        return this == MEMORY_EVICTED || this == DELETED;
    }
}
