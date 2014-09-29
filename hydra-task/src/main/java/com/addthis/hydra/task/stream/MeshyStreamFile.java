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

import java.io.IOException;
import java.io.InputStream;

import java.util.Objects;

import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.stream.StreamSource;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeshyStreamFile implements StreamFile {
    private static final Logger log = LoggerFactory.getLogger(MeshyStreamFile.class);

    final FileReference meshFile;
    final DateTime date;
    final StreamSourceMeshy streamSourceMeshy;

    MeshyStreamFile(StreamSourceMeshy streamSourceMeshy, DateTime date, FileReference meshFile) {
        this.date = date;
        this.meshFile = meshFile;
        this.streamSourceMeshy = streamSourceMeshy;
    }

    @Override
    public String toString() {
        return "{n=" + name() + ",p=" + getPath() + ",u=" + meshFile.getHostUUID() + "}";
    }

    @Override
    public InputStream getInputStream() throws IOException {
        if (streamSourceMeshy.useProcessedTimeRangeMax()) {
            if ((streamSourceMeshy.firstDate == null) || (date.getMillis() < streamSourceMeshy.firstDate.getMillis())) {
                streamSourceMeshy.firstDate = date;
                log.debug("FIRST DATE = {}", streamSourceMeshy.firstDate);
            }
        }
        if ((streamSourceMeshy.lastDate == null) || (date.getMillis() > streamSourceMeshy.lastDate.getMillis())) {
            streamSourceMeshy.lastDate = date;
            log.debug("LAST DATE = {}", streamSourceMeshy.lastDate);
        }
        // this fails on linux with out the explicit cast to InputStream
        return new StreamSource(streamSourceMeshy.meshLink, meshFile.getHostUUID(),
                                meshFile.name, streamSourceMeshy.meshStreamCache).getInputStream();
    }

    @Override
    public long lastModified() {
        return meshFile.lastModified;
    }

    @Override
    public long length() {
        return meshFile.size;
    }

    @Override
    public String name() {
        return meshFile.name;
    }

    @Override
    public String getPath() {
        return streamSourceMeshy.getPathOffset(meshFile.name);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof MeshyStreamFile)) {
            return false;
        }
        MeshyStreamFile otherFile = (MeshyStreamFile) other;
        if (!Objects.equals(meshFile, otherFile.meshFile)) {
            return false;
        }
        if (!Objects.equals(date, otherFile.date)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(meshFile, date);
    }
}
