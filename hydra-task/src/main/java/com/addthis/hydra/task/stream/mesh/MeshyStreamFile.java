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

package com.addthis.hydra.task.stream.mesh;

import java.io.IOException;
import java.io.InputStream;

import java.util.Objects;

import com.addthis.basis.io.IOWrap;

import com.addthis.hydra.data.filter.value.StringFilter;
import com.addthis.hydra.task.stream.StreamFile;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.stream.StreamSource;

import com.ning.compress.lzf.LZFInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import org.xerial.snappy.SnappyInputStream;

import lzma.sdk.lzma.Decoder;
import lzma.streams.LzmaInputStream;

class MeshyStreamFile implements StreamFile {

    private final MeshyClient meshLink;
    private final StringFilter pathFilter;
    final FileReference meshFile;

    MeshyStreamFile(FileReference meshFile, MeshyClient meshLink, StringFilter pathFilter) {
        this.meshLink = meshLink;
        this.pathFilter = pathFilter;
        this.meshFile = meshFile;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        // this fails on linux with out the explicit cast to InputStream
        InputStream in = new StreamSource(meshLink, meshFile.getHostUUID(),
                meshFile.name, 0).getInputStream();
        if (name().endsWith(".gz")) {
            in = IOWrap.gz(in, 4096);
        } else if (name().endsWith(".lzf")) {
            in = new LZFInputStream(in);
        } else if (name().endsWith(".snappy")) {
            in = new SnappyInputStream(in);
        } else if (name().endsWith(".bz2")) {
            in = new BZip2CompressorInputStream(in, true);
        } else if (name().endsWith(".lzma")) {
            in = new LzmaInputStream(in, new Decoder());
        }
        return in;
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
        return pathFilter.filter(meshFile.name);
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
        return true;
    }

    @Override
    public int hashCode() {
        return meshFile.hashCode();
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("path", getPath())
                .add("meshFile", meshFile)
                .toString();
    }
}
