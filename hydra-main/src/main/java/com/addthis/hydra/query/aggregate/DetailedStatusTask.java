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

package com.addthis.hydra.query.aggregate;

import io.netty.util.concurrent.Promise;

public class DetailedStatusTask implements Runnable {

    private final Promise<TaskSourceInfo[]> promise;

    private MeshSourceAggregator sourceAggregator;

    public DetailedStatusTask(Promise<TaskSourceInfo[]> promise) {
        this.promise = promise;
    }

    @Override
    public void run() {
        try {
            promise.trySuccess(taskSourceInfo(sourceAggregator));
        } catch (Exception e) {
            promise.tryFailure(e);
        }
    }

    public void run(MeshSourceAggregator sourceAggregator) {
        this.sourceAggregator = sourceAggregator;
        run();
    }

    public static TaskSourceInfo[] taskSourceInfo(MeshSourceAggregator sourceAggregator) {
        QueryTaskSource[] taskSources = sourceAggregator.taskSources;
        TaskSourceInfo[] taskSourceInfos = new TaskSourceInfo[taskSources.length];
        for (int i = 0; i < taskSources.length; i++) {
            taskSourceInfos[i] = new TaskSourceInfo(taskSources[i]);
        }
        return taskSourceInfos;
    }
}
