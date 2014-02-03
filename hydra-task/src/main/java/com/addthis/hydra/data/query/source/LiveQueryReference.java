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

package com.addthis.hydra.data.query.source;

import java.io.File;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import com.addthis.hydra.data.query.QueryEngine;
import com.addthis.meshy.VirtualFileFilter;
import com.addthis.meshy.VirtualFileInput;
import com.addthis.meshy.VirtualFileReference;

public class LiveQueryReference extends QueryReference {

    private final String job;
    private final QueryEngine queryEngine;

    public LiveQueryReference(File dir, String job, QueryEngine queryEngine) {
        super(dir);
        this.job = job;
        this.queryEngine = queryEngine;
    }

    private boolean isPathValid(String path) {
        return path.endsWith(queryRoot) && path.contains(job);
    }

    @Override
    public long getLastModified() {
        return super.getLastModified() + 1;
    }

    @Override
    public long getLength() {
        return super.getLength() + 1;
    }

    @Override
    public Iterator<VirtualFileReference> listFiles(VirtualFileFilter filter) {
        String path = filter.getToken();
        if (isPathValid(path)) {
            return Collections.singletonList((VirtualFileReference) this).iterator();
        } else {
            return null;
        }
    }

    @Override
    public VirtualFileReference getFile(String name) {
        return isPathValid(name) ? this : null;
    }

    @Override
    public VirtualFileInput getInput(Map<String, String> options) {
        try {
            final DataChannelToInputStream bridge = new DataChannelToInputStream();
            if (options == null) {
                MeshQuerySource.log.warn("Invalid request to getInput.  Options cannot be null");
                return null;
            }
            SearchRunner.querySearchPool.execute(new LiveSearchRunner(options, dirString, bridge, queryEngine));
            return bridge;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
