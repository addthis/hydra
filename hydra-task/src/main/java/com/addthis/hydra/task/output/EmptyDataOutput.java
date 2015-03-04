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
package com.addthis.hydra.task.output;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This output sink <span class="hydra-summary">produces empty output</span>.
 *
 * @user-reference
 * @hydra-name empty
 */
public class EmptyDataOutput extends TaskDataOutput {

    private static final Logger log = LoggerFactory.getLogger(EmptyDataOutput.class);

    private long totalBundles = 0;

    @Override
    protected void open() {}

    @Override
    public void send(Bundle bundle) throws DataChannelError {
        totalBundles++;
    }

    @Override
    public void sendComplete() {
        log.info("[sendComplete] Total bundles: {}", totalBundles);
    }

    @Override
    public void sourceError(Throwable cause) {
        log.error("[sourceError]", cause);
    }

}

