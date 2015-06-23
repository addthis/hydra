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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.addthis.basis.net.HttpUtil;
import com.addthis.basis.net.http.HttpResponse;
import com.addthis.basis.util.Parameter;

import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.alias.AliasManager;
import com.addthis.hydra.util.EmailUtil;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.MoreExecutors;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import org.joda.time.DateTimeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Supports {@code http://} and {@code kick://} callbacks. */
public class JobOnFinishStateHandlerImpl implements JobOnFinishStateHandler {

    private static final Logger log = LoggerFactory.getLogger(JobOnFinishStateHandlerImpl.class);

    private static final int backgroundThreads = Parameter.intValue("spawn.background.threads", 4);
    private static final int backgroundQueueSize = Parameter.intValue("spawn.background.queuesize", 1_000);
    private static final int backgroundEmailMinute = Parameter.intValue("spawn.background.notification.interval.minutes", 60);
    private static final int backgroundHttpTimeoutMs = Parameter.intValue("spawn.background.timeout", 300_000);
    private static final String backgroundEmailAddress = Parameter.value("spawn.background.notification.address");
    private static final String clusterName = Parameter.value("cluster.name", "localhost");

    private final Spawn spawn;

    private final BlockingQueue<Runnable> backgroundTaskQueue = new LinkedBlockingQueue<>(backgroundQueueSize);
    private final ExecutorService backgroundService = MoreExecutors.getExitingExecutorService(
            new ThreadPoolExecutor(backgroundThreads, backgroundThreads, 0L, TimeUnit.MILLISECONDS, backgroundTaskQueue),
            100, TimeUnit.MILLISECONDS);
    private final AtomicLong emailLastFired = new AtomicLong();

    @SuppressWarnings("unused")
    private final Gauge<Integer> backgroundQueueGauge = Metrics.newGauge(
            Spawn.class, "backgroundExecutorQueue", new Gauge<Integer>() {
                public Integer value() {
                    return backgroundTaskQueue.size();
                }
            });

    public JobOnFinishStateHandlerImpl(Spawn spawn) {
        this.spawn = spawn;
    }

    @Override
    public void handle(Job job, JobOnFinishState state) {
        switch (state) {
        case OnComplete:
            doOnState(job, job.getOnCompleteURL(), job.getOnCompleteTimeout(), state);
            break;
        case OnError:
            doOnState(job, job.getOnErrorURL(), job.getOnErrorTimeout(), state);
            break;
        }
    }

    private void doOnState(Job job, String url, int timeoutSecs, JobOnFinishState state) {
        if (Strings.isNullOrEmpty(url)) {
            return;
        }
        if (url.startsWith("http://")) {
            int timeoutMs = timeoutSecs > 0 ? (timeoutSecs * 1000) : backgroundHttpTimeoutMs;
            try {
                Runnable task = createBackgroundTask(job, state, url, timeoutMs);
                backgroundService.submit(task);
            } catch (Exception e) {
                log.error("Error making background call: {}", e.getMessage(), e);
                emailNotification(job.getId(), state, Throwables.getStackTraceAsString(e));
            }
        } else if (url.startsWith("kick://")) {
            AliasManager aliasManager = spawn.getAliasManager();
            for (String kick : Splitter.on(",").omitEmptyStrings().trimResults().split(url.substring(7))) {
                List<String> jobIds = aliasManager.aliasToJobs(kick);
                if (jobIds != null) {
                    for (String jobId : jobIds) {
                        safeStartJob(jobId.trim());
                    }
                } else {
                    safeStartJob(kick);
                }
            }
        } else {
            log.warn("invalid {} url: {} for job {}", state, url, job.getId());
        }
    }

    private Runnable createBackgroundTask(
            Job job,
            final JobOnFinishState state,
            final String url,
            final int timeoutMs) throws Exception {

        final String jobId = job.getId();
        final byte[] jobJson = CodecJSON.encodeString(job).getBytes();
        return () -> {
            try {
                HttpResponse response = HttpUtil.httpPost(url, "javascript/text", jobJson, timeoutMs);
                if (response.getStatus() >= 400) {
                    String err = "HTTP POST to " + url + " in background task \"" + jobId + " "
                            + state + "\" returned " + response.getStatus() + " " + response.getReason();
                    log.error(err);
                    emailNotification(jobId, state, err);
                }
            } catch (IOException ex) {
                log.error("IOException when attempting to contact \"{}\" in background task \"{} {}\"",
                        url, jobId, state, ex);
                emailNotification(jobId, state, Throwables.getStackTraceAsString(ex));
            }
        };
    }
    
    private void emailNotification(String jobId, JobOnFinishState state, String body) {
        if (backgroundEmailAddress != null) {
            long currentTime = System.currentTimeMillis();
            long lastTime = emailLastFired.get();
            long elapse = currentTime - lastTime;
            if (elapse > (long) backgroundEmailMinute * DateTimeConstants.MILLIS_PER_MINUTE) {
                EmailUtil.email(backgroundEmailAddress, emailSubject(jobId, state), body);
                emailLastFired.set(currentTime);
            }
        }
    }

    private String emailSubject(String jobId, JobOnFinishState state) {
        return "Background operation failed -" + clusterName + "- {" + jobId + " " + state.name() + "}";
    }

    private void safeStartJob(String jobId) {
        try {
            spawn.startJob(jobId, 0);
        } catch (Exception ex) {
            log.warn("[safe.start] {} failed due to {}", jobId, ex.getMessage(), ex);
        }
    }

}
