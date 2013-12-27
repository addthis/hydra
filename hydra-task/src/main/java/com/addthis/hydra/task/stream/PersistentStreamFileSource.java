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

import java.io.File;
import java.io.IOException;

/**
 * Extension of {@link StreamFileSource} that adds additional
 * shutdown and initialization methods required for persistent
 * sources that need to track meta-data between executions.
 */
public interface PersistentStreamFileSource extends StreamFileSource {

    /**
     * @return true if this source has a mod defined
     */
    public boolean hasMod();

    /**
     * @param markDir - the directory where state for this source is maintained
     * @param shards  - the set of shards from the upstream source to consume, may be null
     * @return true if initialized successfully
     * @throws Exception
     */
    public boolean init(File markDir, Integer[] shards) throws Exception;

    /**
     * Shutdown closable resources and write meta data to the state directory
     *
     * @throws IOException
     */
    public void shutdown() throws IOException;
}
