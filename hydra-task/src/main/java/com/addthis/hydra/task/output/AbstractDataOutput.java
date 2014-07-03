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

import java.io.IOException;

import java.net.ServerSocket;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.jmx.MBeanRemotingSupport;
import com.addthis.basis.util.Parameter;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec; import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.filter.bundle.BundleFilter;
import com.addthis.hydra.task.map.DataPurgeConfig;
import com.addthis.hydra.task.map.DataPurgeService;
import com.addthis.hydra.task.map.DataPurgeServiceImpl;
import com.addthis.hydra.task.run.TaskRunConfig;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.joda.time.DateTime;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

public abstract class AbstractDataOutput extends DataOutputTypeList {

    private final Logger log = LoggerFactory.getLogger(AbstractDataOutput.class);

    @FieldConfig(codable = true)
    private boolean enableJmx = Parameter.boolValue("split.minion.usejmx", true);

    /**
     * Array of strings that determines the output file path.
     * See above for variable substitutions.
     * This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String[] path;

    /**
     * Optional filter to apply to the data stream.
     * Only bundles from the stream that return true
     * are emitted to the output. Default is null.
     */
    @FieldConfig(codable = true)
    private BundleFilter filter;

    /**
     * Optionally purge data from previous runs of this job.
     * Purging is based on the date of the data.
     * This field is optional.
     */
    @FieldConfig(codable = true)
    private DataPurgeConfig dataPurgeConfig;

    private MBeanRemotingSupport jmxremote;

    private String[] fileToken;
    private TokenIndex[] varToken;

    abstract AbstractOutputWriter getWriter();

    @Override
    public void open(TaskRunConfig config) {
        log.info("[init] " + config);

        // todo: Reduce duplication with TreeMapper, do earlier in
        // call tree (ie before we have any output)
        if (enableJmx) {
            try {
                //          jmxname = new ObjectName("com..hydra:type=Hydra,node=" + queryPort);
                //ManagementFactory.getPlatformMBeanServer().registerMBean(mapstats, jmxname);
                ServerSocket ss = new ServerSocket();
                ss.setReuseAddress(true);
                ss.bind(null);
                int jmxport = ss.getLocalPort();
                ss.close();
                if (jmxport == -1) {
                    log.warn("[init.jmx] failed to get a port");
                } else {
                    try {
                        jmxremote = new MBeanRemotingSupport(jmxport);
                        jmxremote.start();
                        log.info("[init.jmx] port=" + jmxport);
                    } catch (Exception e) {
                        log.error("[init.jmx]", e);
                    }
                }
            } catch (IOException e)  {
                log.error("", e);
            }
        }
        if (dataPurgeConfig != null) {
            purgeData();
        }
        LinkedList<TokenIndex> vt = new LinkedList<TokenIndex>();
        String ft[] = new String[path.length];
        for (int i = 0; i < ft.length; i++) {
            if (path[i].startsWith("[[") && path[i].endsWith("]]")) {
                String tok = path[i].substring(2, path[i].length() - 2);
                if (tok.equals("job")) {
                    path[i] = config.jobId;
                } else if (tok.startsWith("node")) {
                    path[i] = Integer.toString(tok.startsWith("nodes") ? config.nodeCount : config.node);
                    int cp = tok.indexOf(":");
                    if (cp > 0) {
                        path[i] = padleft(path[i], Integer.parseInt(tok.substring(cp + 1)));
                    }
                }
            }
            if (path[i].startsWith("{{") && path[i].endsWith("}}")) {
                String tok = path[i].substring(2, path[i].length() - 2);
                vt.add(new TokenIndex(getFormat().getField(tok), i));
            }
            ft[i] = new String(path[i]);
            if (log.isDebugEnabled()) log.debug("[binding " + path[i]);
        }
        if (log.isDebugEnabled()) log.debug("[bind] var=" + vt);
        varToken = vt.toArray(new TokenIndex[vt.size()]);
        fileToken = ft;
    }

    @Override
    public void send(Bundle bundle) throws DataChannelError {
        try {
            AbstractOutputWriter writer = getWriter();
            if (filter != null && !filter.filter(bundle)) {
                return;
            }
            String tok[] = new String[fileToken.length];
            for (int i = 0; i < tok.length; i++) {
                tok[i] = fileToken[i];
            }
            for (int i = 0; i < varToken.length; i++) {
                TokenIndex t = varToken[i];
                ValueObject vo = bundle.getValue(bundle.getFormat().getField(t.field.getName()));
                if (vo == null) {
                    throw new NullPointerException("Null value in output path: " + t.field.getName());
                }
                tok[t.index] = ValueUtil.asNativeString(vo);
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < tok.length; i++) {
                sb.append(tok[i]);
            }
            if (log.isDebugEnabled()) log.debug("send " + bundle + " to " + sb);
            writer.writeLine(sb.toString(), bundle);
        } catch (Exception ex) {
            throw DataChannelError.promote(ex);
        }
    }

    @Override
    public void send(List<Bundle> bundles) {
        if (bundles != null && !bundles.isEmpty()) {
            for (Bundle bundle : bundles) {
                send(bundle);
            }
        }
    }

    @Override
    public void sourceError(DataChannelError er) {
        AbstractOutputWriter writer = getWriter();
        writer.closeOpenOutputs();
        log.error("[sourceError] " + er);
    }

    @Override
    public void sendComplete() {
        AbstractOutputWriter writer = getWriter();
        writer.closeOpenOutputs();
        if (jmxremote != null) {
            try {
                jmxremote.stop();
                jmxremote = null;
            } catch (IOException e)  {
                log.error("", e);
            }
        }
        log.info("[sendComplete]");
    }

    public AbstractDataOutput setPath(String[] path) {
        this.path = path;
        return this;
    }

    /**
     * for associating a bundle field with a mutable string index
     */
    class TokenIndex {

        TokenIndex(BundleField field, int index) {
            this.field = field;
            this.index = index;
        }

        BundleField field;
        int index;

        @Override
        public String toString() {
            return field.getName() + "@" + index;
        }
    }

    private void purgeData() {
        if ((dataPurgeConfig.getMaxAgeInDays() > 0 || dataPurgeConfig.getMaxAgeInHours() > 0) && dataPurgeConfig.getDatePathFormat() != null) {
            log.info("Starting DataPurge in separate thread...");
            /* run the data purge in a separate thread to allow splitting to run concurrently with the purging process */
            ExecutorService executorService = MoreExecutors.getExitingExecutorService(new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactoryBuilder().setNameFormat("DataOutputFilePurger-%d").build()));
            executorService.execute(new DataPurgeRunner(new DataPurgeServiceImpl()));
            executorService.shutdown();
        } else {
            if (log.isDebugEnabled()) {
                log.debug("DataPurge parameters not provided, data will be maintained indefinitely.");
            }
        }
    }

    private class DataPurgeRunner implements Runnable {

        private DataPurgeService dataPurgeService;

        private DataPurgeRunner(DataPurgeService dataPurgeService) {
            this.dataPurgeService = dataPurgeService;
        }

        @Override
        public void run() {
            if (!dataPurgeService.purgeData(dataPurgeConfig, new DateTime())) {
                log.info("Unable to purge data older than " + dataPurgeConfig.getMaxAgeInDays() + " days");
            } else {
                log.info("Successfully purged data older than " + dataPurgeConfig.getMaxAgeInDays() + " days");
            }
        }
    }

    public static String padleft(String str, int len) {
        final String pad = "0000000000";
        if (str.length() < len) {
            return pad.substring(pad.length() - len + str.length()).concat(str);
        } else if (str.length() > len) {
            return str.substring(0, len);
        } else {
            return str;
        }
    }
}
