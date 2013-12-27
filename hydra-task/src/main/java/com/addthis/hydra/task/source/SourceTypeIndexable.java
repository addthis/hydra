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

import com.addthis.bundle.channel.DataChannelSource;


public interface SourceTypeIndexable extends DataChannelSource {

    /**
     * return true if source supports skip and skip completed.
     * returns false otherwise and source must start at the beginning.
     */
    public abstract boolean setOffset(long offset);

    /**
     * return current offset as a long.
     */
    public abstract long getOffset();
}
