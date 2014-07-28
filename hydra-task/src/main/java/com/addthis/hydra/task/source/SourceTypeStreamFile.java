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

import com.addthis.hydra.task.stream.StreamFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class SourceTypeStreamFile extends AbstractDataSourceWrapper implements SourceTypeStateful {

    private static final Logger log = LoggerFactory.getLogger(SourceTypeStreamFile.class);

    private final StreamFile streamFile;

    public SourceTypeStreamFile(TaskDataSource source, StreamFile streamFile) {
        super(source);
        log.debug("<init> wrap source={} stream={}", source, streamFile);
        this.streamFile = streamFile;
    }

    @Override
    public String getSourceIdentifier() {
        return streamFile.getPath();
    }

    @Override
    public String getSourceStateMarker() {
        return streamFile.lastModified() + "/" + streamFile.length();
    }

}
