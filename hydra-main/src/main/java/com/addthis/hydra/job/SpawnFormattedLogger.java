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
package com.addthis.hydra.job;

import javax.annotation.Nullable;

import java.io.File;

import java.util.Map;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.hydra.task.output.DataOutputFile;
import com.addthis.hydra.task.output.DataOutputTypeList;
import com.addthis.hydra.task.output.OutputStreamChannel;
import com.addthis.hydra.task.output.OutputWrapperFactory;
import com.addthis.hydra.task.output.OutputWriter;

import org.joda.time.DateTime;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public class SpawnFormattedLogger {

    public enum EventType {
        JOB_FINISH,
        SPAWN_STATE
    }


    private static Logger log = LoggerFactory.getLogger(SpawnFormattedLogger.class);
    private static String clusterName = Parameter.value("cluster.name", "localhost");
    public static final String[] LOG_PATH = {
            "{{DATE_YEAR}}", "/", "{{DATE_MONTH}}", "/", "{{DATE_DAY}}", "/", "{{FILE_PREFIX_HOUR}}"};

    @Nullable
    private final DataOutputTypeList bundleLog;

    private SpawnFormattedLogger() {
        log.info("Creating the null-based spawn formatted logger - no output emitted.");
        bundleLog = null;
    }

    private SpawnFormattedLogger(File file) {
        log.info("Creating the file-based spawn formatted logger.");
        DataOutputTypeList newOutputSink = null;
        try {
            String absPath = file.getAbsolutePath();
            OutputWrapperFactory factory = new OutputWrapperFactory(absPath);
            OutputWriter writer = new OutputWriter();
            writer.setMaxOpen(1).setOutputWrapperFactory(factory).setFormat(new OutputStreamChannel());
            writer.postDecode();
            newOutputSink = new DataOutputFile().setWriter(writer).setPath(LOG_PATH);
            newOutputSink.init(null);
        } catch (Exception ex)  {
            log.error("", ex);
        } finally {
            bundleLog = newOutputSink;
        }
    }

    static SpawnFormattedLogger createFileBasedLogger(File file) {
        return new SpawnFormattedLogger(file);
    }

    static SpawnFormattedLogger createNullLogger() {
        return new SpawnFormattedLogger();
    }


    private void bundleSetValue(Bundle bundle, String field, String value) {
        BundleFormat format = bundleLog.getFormat();
        bundle.setValue(format.getField(field), ValueFactory.create(value));
    }

    private void bundleSetValue(Bundle bundle, String field, int value) {
        BundleFormat format = bundleLog.getFormat();
        bundle.setValue(format.getField(field), ValueFactory.create(value));
    }

    private void bundleSetValue(Bundle bundle, String field, long value) {
        BundleFormat format = bundleLog.getFormat();
        bundle.setValue(format.getField(field), ValueFactory.create(value));
    }

    private void bundleSetValue(Bundle bundle, String field, boolean value) {
        BundleFormat format = bundleLog.getFormat();
        bundle.setValue(format.getField(field), ValueFactory.create(value ? 1 : 0));
    }


    private Bundle initBundle(EventType event) {
        assert (bundleLog != null);
        Bundle bundle = bundleLog.createBundle();
        bundleSetValue(bundle, "CLUSTER", clusterName);
        bundleSetValue(bundle, "EVENT_TYPE", event.toString());
        long time = JitterClock.globalTime();
        DateTime dateTime = new DateTime(time);
        bundleSetValue(bundle, "TIME", time);
        bundleSetValue(bundle, "DATE_YEAR", dateTime.getYear());
        bundleSetValue(bundle, "DATE_MONTH", dateTime.getMonthOfYear());
        bundleSetValue(bundle, "DATE_DAY", dateTime.getDayOfMonth());
        bundleSetValue(bundle, "FILE_PREFIX_HOUR", "logger-" + String.format("%02d", dateTime.getHourOfDay()));
        return bundle;
    }

    /**
     * Emit to the output periodic statistics about the cluster that are collected by Spawn.
     */
    void periodicState(Map<String, Long> events) {
        if (bundleLog != null) {
            try {
                Bundle bundle = initBundle(EventType.SPAWN_STATE);
                for (Map.Entry<String, Long> entry : events.entrySet()) {
                    bundleSetValue(bundle, entry.getKey().toUpperCase(), entry.getValue());
                }
                bundleLog.send(bundle);
            } catch (Exception ex)  {
                log.error("", ex);
            }
        }
    }

    /**
     * Emit to the output sink information about the completion of a job.
     */
    void finishJob(Job job) {
        if (bundleLog != null) {
            try {
                Bundle bundle = initBundle(EventType.JOB_FINISH);
                BundleFormat format = bundleLog.getFormat();
                bundleSetValue(bundle, "JOB_ID", job.getId());
                bundleSetValue(bundle, "JOB_STATE", job.getState().toString());
                bundleSetValue(bundle, "JOB_WAS_STOPPED", job.getWasStopped());
                Long start = job.getStartTime();
                Long end = job.getEndTime();
                if (start != null) {
                    bundleSetValue(bundle, "JOB_START_TIME", start);
                }
                if (end != null) {
                    bundleSetValue(bundle, "JOB_END_TIME", end);
                }
                if (start != null && end != null) {
                    bundleSetValue(bundle, "JOB_ELAPSED_TIME", end - start);
                }
                int taskCount = job.getTaskCount();
                bundleSetValue(bundle, "JOB_TASK_COUNT", taskCount);
                ValueArray taskMeanRates = ValueFactory.createArray(taskCount);
                for (int i = 0; i < taskCount; i++) {
                    JobTask task = job.getTask(i);
                    taskMeanRates.add(ValueFactory.create(task.getMeanRate()));
                }
                bundle.setValue(format.getField("TASK_AVG_RATES"), taskMeanRates);
                bundleLog.send(bundle);
            } catch (Exception ex)  {
                log.error("", ex);
            }
        }
    }

    void close() {
        if (bundleLog != null) {
            bundleLog.sendComplete();
        }
    }

}
