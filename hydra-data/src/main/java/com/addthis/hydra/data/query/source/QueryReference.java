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

import java.util.Iterator;
import java.util.Map;

import com.addthis.basis.util.Parameter;

import com.addthis.meshy.VirtualFileFilter;
import com.addthis.meshy.VirtualFileInput;
import com.addthis.meshy.VirtualFileReference;

/**
 * virtualizes queries
 * <p/>
 * This class's getInput method is the main entry point for all queries to this mqworker. It is the first class in the three
 * step query process.
 * <p/>
 * By virtualizes queries, we mean that it performs queries and gets a response in a way that meshy can understand. As the
 * class is named 'MeshQuerySource', this makes sense. As the primary class and entry point for mq worker function, it might be
 * worth this explanation to prevent any confusion.
 */
class QueryReference implements VirtualFileReference {

    static final String queryRoot = Parameter.value("mesh.query.root", "query");

    //File that contains the next parent id to be assigned in the query tree, but this parameter does not
    //  actually control that.
    private static final String queryReferenceFileName = Parameter.value("meshQuerySource.queryReferenceFile", "nextID");

    final File dir;
    final String dirString;
    final File queryReferenceFile;

    QueryReference(final File dir) {
        try {
            this.dir = dir.getCanonicalFile();
            this.dirString = dir.toString();
            queryReferenceFile = new File(dir, queryReferenceFileName);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public String toString() {
        return dirString;
    }

    @Override
    public String getName() {
        return queryRoot;
    }

    @Override
    public long getLastModified() {
        return queryReferenceFile.lastModified();
    }

    @Override
    public long getLength() {
        return queryReferenceFile.length();
    }

    @Override
    public Iterator<VirtualFileReference> listFiles(VirtualFileFilter filter) {
        return null;
    }

    @Override
    public VirtualFileReference getFile(String name) {
        return null;
    }

    /**
     * Submits the query to the query search pool as a SearchRunner and creates the bridge that will
     * hand the query response data to meshy.
     *
     * @param options
     * @return the response bridge (DataChannelToInputStream)
     */
    @Override
    public VirtualFileInput getInput(Map<String, String> options) {
        try {
            final DataChannelToInputStream bridge = new DataChannelToInputStream();
            if (options == null) {
                MeshQuerySource.log.warn("Invalid request to getInput.  Options cannot be null");
                return null;
            }
            final String flag = options.get("flag");
            if (flag != null) {
                if (flag.equals("die")) {
                    System.exit(1);
                } else if (flag.equals("DIE")) {
                    Runtime.getRuntime().halt(1);
                }
            }
            SearchRunner.querySearchPool.submit(new SearchRunner(options, dirString, bridge));
            return bridge;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
