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
package com.addthis.hydra.job.spawn;

import java.io.IOException;

import com.addthis.basis.net.HttpUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BackgroundPost implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(BackgroundPost.class);

    private final String jobId;
    private final String state;
    private final String url;
    private final int timeout;
    private final byte[] post;

    BackgroundPost(String jobId, String state, String url, int timeout, byte[] post) {
        this.jobId = jobId;
        this.state = state;
        this.url = url;
        this.timeout = timeout;
        this.post = post;
    }

    @Override
    public void run() {
        try {
            // timeout is passed as seconds and must be converted to milliseconds
            int postTimeout = timeout > 0 ? (timeout * 1000) : Spawn.backgroundHttpTimeout;
            HttpUtil.httpPost(url, "javascript/text", post, postTimeout);
        } catch (IOException ex) {
            log.error("IOException when attempting to contact \"{}\" in background task \"{} {}\"",
                      url, jobId, state, ex);
            Throwable cause = ex.getCause();
            if (cause != null) {
                log.error("Caused by ", cause);
            }
            Spawn.emailNotification(jobId, state, ex);
        }
    }
}
