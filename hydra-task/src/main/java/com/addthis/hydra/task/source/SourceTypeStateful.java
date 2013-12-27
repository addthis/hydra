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


public interface SourceTypeStateful extends DataChannelSource {

    /**
     * @return a unique, durable identifier for this source
     */
    public abstract String getSourceIdentifier();

    /**
     * @return a unique string indicating source state.  if the source acquires new data
     *         since it was last opened, the source must return a new unique string.
     */
    public abstract String getSourceStateMarker();
}
