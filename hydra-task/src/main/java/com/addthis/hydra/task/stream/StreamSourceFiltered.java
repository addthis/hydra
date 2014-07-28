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

import com.addthis.hydra.data.filter.value.StringFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class StreamSourceFiltered implements StreamFileSource {

    private static final Logger log = LoggerFactory.getLogger(StreamSourceFiltered.class);

    private final StreamFileSource wrap;
    private final StringFilter filter;

    public StreamSourceFiltered(StreamFileSource wrap, StringFilter filter) {
        this.wrap = wrap;
        this.filter = filter;
    }

    @Override
    public StreamFile nextSource() {
        StreamFile next = null;
        while ((next = wrap.nextSource()) != null) {
            String path = next.getPath();
            String result = filter.filter(path);
            if (result == null) {
                if (log.isDebugEnabled()) log.debug("filter rejected " + next);
                continue;
            }
            if (path == result) {
                if (log.isDebugEnabled()) log.debug("filter match " + next);
                return next;
            } else {
                next = new FilteredStreamFile(next, result);
                if (log.isDebugEnabled()) log.debug("filter match " + next);
                return next;
            }
        }
        return null;
    }

    static class FilteredStreamFile implements StreamFile {

        private final StreamFile wrap;
        private final String path;

        public FilteredStreamFile(StreamFile wrap, String path) {
            this.wrap = wrap;
            this.path = path;
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return wrap.getInputStream();
        }

        @Override
        public long lastModified() {
            return wrap.lastModified();
        }

        @Override
        public long length() {
            return wrap.length();
        }

        @Override
        public String name() {
            return wrap.name();
        }

        @Override
        public String getPath() {
            return path;
        }
    }
}
