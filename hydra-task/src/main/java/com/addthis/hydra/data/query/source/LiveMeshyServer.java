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

import java.io.IOException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.Parameter;

import com.addthis.meshy.MeshyServer;
import com.addthis.meshy.VirtualFileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiveMeshyServer extends MeshyServer {

    private static final Logger log = LoggerFactory.getLogger(LiveMeshyServer.class);
    private static final int LIVE_QUERY_WAIT = Parameter.intValue("mapper.live.wait", 60);

    private final AtomicBoolean liveClosed = new AtomicBoolean(false);
    private final LiveQueryFileSystem liveQueryFileSystem;

    public LiveMeshyServer(int port, LiveQueryReference queryReference) throws IOException {
        super(port);
        this.liveQueryFileSystem = new LiveQueryFileSystem(queryReference);
    }

    @Override
    public VirtualFileSystem[] getFileSystems() {
        if (liveClosed.get()) {
            return new VirtualFileSystem[] {};
        }
        return new VirtualFileSystem[] {liveQueryFileSystem};
    }

    @Override
    public void close() {
        liveClosed.set(true);
        liveQueryFileSystem.getFileRoot().engine().closeWhenIdle();
        log.info("Shutting down live query server. Disabled finding this job from mqm.");
        try {
            log.info("Going to wait up to {} seconds for any queries still running.", LIVE_QUERY_WAIT);
            SearchRunner.querySearchPool.shutdown();
            if (SearchRunner.querySearchPool.awaitTermination(LIVE_QUERY_WAIT, TimeUnit.SECONDS)) {
                log.info("Running queries all finished okay.");
            } else {
                log.warn("Queries did not finish, so they will be interrupted.");
                SearchRunner.querySearchPool.shutdownNow();
            }
        } catch (InterruptedException ignored) {
        }
        super.close();
    }
}
