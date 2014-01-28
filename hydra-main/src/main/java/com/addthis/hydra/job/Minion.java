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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;

import java.lang.management.ManagementFactory;

import java.net.InetAddress;
import java.net.ServerSocket;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.Backoff;
import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Numbers;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.SimpleExec;
import com.addthis.basis.util.Strings;

import com.addthis.bark.ZkClientFactory;
import com.addthis.bark.ZkGroupMembership;
import com.addthis.bark.ZkHelpers;
import com.addthis.codec.Codec;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.job.backup.DailyBackup;
import com.addthis.hydra.job.backup.GoldBackup;
import com.addthis.hydra.job.backup.HourlyBackup;
import com.addthis.hydra.job.backup.MonthlyBackup;
import com.addthis.hydra.job.backup.ScheduledBackupType;
import com.addthis.hydra.job.backup.WeeklyBackup;
import com.addthis.hydra.job.mq.CommandTaskDelete;
import com.addthis.hydra.job.mq.CommandTaskKick;
import com.addthis.hydra.job.mq.CommandTaskNew;
import com.addthis.hydra.job.mq.CommandTaskReplicate;
import com.addthis.hydra.job.mq.CommandTaskRevert;
import com.addthis.hydra.job.mq.CommandTaskStop;
import com.addthis.hydra.job.mq.CommandTaskUpdateReplicas;
import com.addthis.hydra.job.mq.CoreMessage;
import com.addthis.hydra.job.mq.HostCapacity;
import com.addthis.hydra.job.mq.HostMessage;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.mq.JobKey;
import com.addthis.hydra.job.mq.JobMessage;
import com.addthis.hydra.job.mq.ReplicaTarget;
import com.addthis.hydra.job.mq.StatusTaskBackup;
import com.addthis.hydra.job.mq.StatusTaskBegin;
import com.addthis.hydra.job.mq.StatusTaskCantBegin;
import com.addthis.hydra.job.mq.StatusTaskEnd;
import com.addthis.hydra.job.mq.StatusTaskPort;
import com.addthis.hydra.job.mq.StatusTaskReplica;
import com.addthis.hydra.job.mq.StatusTaskReplicate;
import com.addthis.hydra.job.mq.StatusTaskRevert;
import com.addthis.hydra.mq.MessageConsumer;
import com.addthis.hydra.mq.MessageListener;
import com.addthis.hydra.mq.MessageProducer;
import com.addthis.hydra.mq.RabbitMessageConsumer;
import com.addthis.hydra.mq.RabbitMessageProducer;
import com.addthis.hydra.mq.SessionExpireListener;
import com.addthis.hydra.mq.ZKMessageProducer;
import com.addthis.hydra.mq.ZkSessionExpirationHandler;
import com.addthis.hydra.task.run.TaskExitState;
import com.addthis.hydra.util.MetricsServletMaker;
import com.addthis.hydra.util.MinionWriteableDiskCheck;
import com.addthis.maljson.JSONObject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import org.I0Itec.zkclient.ZkClient;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * TODO implement APIs for extended probing, sanity, clearing of job state
 */
public class Minion extends AbstractHandler implements MessageListener, ZkSessionExpirationHandler, Codec.Codable {

    private static Logger log = LoggerFactory.getLogger(Minion.class);
    private static int webPort = Parameter.intValue("minion.web.port", 5051);
    private static int minJobPort = Parameter.intValue("minion.job.baseport", 0);
    private static int maxJobPort = Parameter.intValue("minion.job.maxport", 0);
    private static ReentrantLock capacityLock = new ReentrantLock();
    private final java.util.Set<String> activeTaskKeys;
    private static String dataDir = System.getProperty("minion.data.dir", "minion");
    private static String group = System.getProperty("minion.group", "none");
    private static String localHost = System.getProperty("minion.localhost");
    private static boolean linkBackup = !System.getProperty("minion.backup.hardlink", "0").equals("0");
    private static final DateTimeFormatter timeFormat = DateTimeFormat.forPattern("yyMMdd-HHmmss");
    private static final String batchBrokerHost = Parameter.value("batch.brokerHost", "localhost");
    private static final String batchBrokerPort = Parameter.value("batch.brokerPort", "5672");
    private static final int mqReconnectDelay = Parameter.intValue("mq.reconnect.delay", 10000);
    private static final int mqReconnectTries = Parameter.intValue("mq.reconnect.tries", 10);
    private static final int sendStatusRetries = Parameter.intValue("send.status.retries", 5);
    private static final int sendStatusRetryDelay = Parameter.intValue("send.status.delay", 5000);
    private static final long hostMetricUpdaterInterval = Parameter.longValue("minion.host.metric.interval", 30 * 1000);
    private static final String remoteConnectMethod = Parameter.value("minion.remote.connect.method", "ssh -o StrictHostKeyChecking=no -o TCPKeepAlive=yes -o ServerAliveInterval=30");
    private static final String rsyncCommand = Parameter.value("minion.rsync.command", "rsync");
    private static final int maxActiveTasks = Parameter.intValue("minion.max.active.tasks", 3);
    private static final int copyRetryLimit = Parameter.intValue("minion.copy.retry.limit", 3);
    private static final int copyRetryDelaySeconds = Parameter.intValue("minion.copy.retry.delay", 10);
    private static ReentrantLock revertLock = new ReentrantLock();

    private static String cpcmd = "cp";
    private static String lncmd = "ln";
    private static String lscmd = "ls";
    private static String rmcmd = "rm";
    private static String mvcmd = "mv";
    private static String ducmd = "du";
    private static String echoWithDate_cmd = "echo `date '+%y/%m/%d %H:%M:%S'` ";

    public static final String MINION_ZK_PATH = "/minion/";
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ExecutorService messageTaskExecutorService = MoreExecutors.getExitingExecutorService(
            new ThreadPoolExecutor(4, 4, 100L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()));
    // This next executor service only serves promote/demote requests, so that these will be performed quickly and not
    // wait on a lengthy revert / delete / etc.
    private final ExecutorService promoteDemoteTaskExecutorService = MoreExecutors.getExitingExecutorService(
            new ThreadPoolExecutor(4, 4, 100L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()));
    private Lock minionStateLock = new ReentrantLock();
    @Codec.Set(codable = true)
    private MinionTaskDeleter minionTaskDeleter;
    // Historical metrics
    private Timer fileStatsTimer;
    private Counter sendStatusFailCount;
    private Counter sendStatusFailAfterRetriesCount;
    private final Backoff jobConsumerBackoff = new Backoff(1000, 60000);

    private final int replicateCommandDelaySeconds = Parameter.intValue("replicate.cmd.delay.seconds", 0);
    private final int backupCommandDelaySeconds = Parameter.intValue("backup.cmd.delay.seconds", 0);

    private boolean useMacFriendlyPSCommands = false;

    private static final String defaultMinionType = "default";

    // detect fl-cow in sys env and apple for copy command
    {
        for (String v : System.getenv().values()) {
            if (v.toLowerCase().contains("libflcow")) {
                log.info("detected support for copy-on-write hard-links");
                linkBackup = true;
                break;
            }
        }
        for (Object v : System.getProperties().values()) {
            if (v.toString().toLowerCase().contains("apple") || v.toString().toLowerCase().contains("mac os x")) {
                log.info("detected darwin-based system. switching to gnu commands");
                cpcmd = "gcp";
                lncmd = "gln";
                lscmd = "gls";
                rmcmd = "grm";
                mvcmd = "gmv";
                ducmd = "gdu";
                useMacFriendlyPSCommands = true;
                break;
            }
        }
    }

    public static void main(String args[]) throws Exception {
        new Minion(new File(args.length > 0 ? args[0] : dataDir), args.length > 2 ? Integer.parseInt(args[1]) : webPort);
    }

    private final File rootDir;
    private final File stateFile;
    private final File liveEverywhereMarkerFile;
    private final String myHost;
    private long startTime;
    private int nextPort;
    private String user;
    private String path;
    private TaskRunner runner;
    private final ConcurrentHashMap<String, JobTask> tasks = new ConcurrentHashMap<>();
    private final Object jmsxmitlock = new Object();
    private final AtomicLong diskTotal = new AtomicLong(0);
    private final AtomicLong diskFree = new AtomicLong(0);
    private final Server jetty;
    private final ServletHandler metricsHandler;
    private final boolean readOnly;
    private boolean diskReadOnly;
    private MinionWriteableDiskCheck diskHealthCheck;
    private int minionPid = -1;

    @Codec.Set(codable = true, required = true)
    private String uuid;
    @Codec.Set(codable = true)
    private ConcurrentHashMap<String, Integer> stopped = new ConcurrentHashMap<>();
    @Codec.Set(codable = true)
    private final ArrayList<CommandTaskKick> jobQueue = new ArrayList<>(10);
    @Codec.Set(codable = true)
    private String minionTypes;

    private Histogram activeTaskHistogram;

    @VisibleForTesting
    public Minion() {
        uuid = UUID.randomUUID().toString();

        // null placeholder for now
        this.rootDir = null;
        this.nextPort = 0;
        this.startTime = 0;
        this.stateFile = null;
        this.liveEverywhereMarkerFile = null;
        this.myHost = null;
        this.user = null;
        this.path = null;
        this.jetty = null;
        this.metricsHandler = null;
        this.readOnly = false;
        this.diskReadOnly = false;
        this.minionPid = -1;
        this.activeTaskKeys = new HashSet<>();
    }

    @VisibleForTesting
    public Minion(File rootDir, int port) throws Exception {
        this.rootDir = rootDir;
        this.nextPort = minJobPort;
        this.startTime = System.currentTimeMillis();
        this.stateFile = new File(Files.initDirectory(rootDir), "minion.state");
        this.liveEverywhereMarkerFile = new File(rootDir, "liveeverywhere.marker");
        this.myHost = (localHost != null ? localHost : InetAddress.getLocalHost().getHostAddress());
        this.user = new SimpleExec("whoami").join().stdoutString().trim();
        this.path = rootDir.getAbsolutePath();
        this.diskTotal.set(rootDir.getTotalSpace());
        this.diskFree.set(rootDir.getFreeSpace());
        this.diskReadOnly = false;
        this.readOnly = Boolean.getBoolean("minion.readOnly");
        minionTaskDeleter = new MinionTaskDeleter();
        if (stateFile.exists()) {
            CodecJSON.decodeString(this, Bytes.toString(Files.read(stateFile)));
        } else {
            uuid = UUID.randomUUID().toString();
        }
        File minionTypes = new File(rootDir, "minion.types");
        this.minionTypes = minionTypes.exists() ? new String(Files.read(minionTypes)).replaceAll("\n", "") : defaultMinionType;
        this.activeTaskKeys = new HashSet<>();
        jetty = new Server(webPort);
        jetty.setHandler(this);
        jetty.start();
        waitForJetty();
        sendStatusFailCount = Metrics.newCounter(Minion.class, "sendStatusFail-" + getJettyPort() + "-JMXONLY");
        sendStatusFailAfterRetriesCount = Metrics.newCounter(Minion.class, "sendStatusFailAfterRetries-" + getJettyPort() + "-JMXONLY");
        fileStatsTimer = Metrics.newTimer(Minion.class, "JobTask-byte-size-timer");
        this.metricsHandler = MetricsServletMaker.makeHandler();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    jetty.stop();
                    minionTaskDeleter.stopDeletionThread();
                    if (zkClient != null) {
                        minionGroupMembership.removeFromGroup("/minion/up", getUUID());
                        zkClient.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        activeTaskHistogram = Metrics.newHistogram(Minion.class, "activeTasks");
        new HostMetricUpdater();
        try {
            joinGroup();
            connectToMQ();
            updateJobsMeta(rootDir);
            if (liveEverywhereMarkerFile.createNewFile()) {
                log.warn("cutover to live-everywhere tasks");
            }
            writeState();
            runner = new TaskRunner();
            runner.start();
            this.diskHealthCheck = new MinionWriteableDiskCheck(this);
            this.diskHealthCheck.startHealthCheckThread();
            sendHostStatus();
            log.info("[init] up on " + myHost + ":" + getJettyPort() + " as " + user + " in " + path);
            String processName = ManagementFactory.getRuntimeMXBean().getName();
            minionPid = Integer.valueOf(processName.substring(0, processName.indexOf("@")));
            log.info("[minion.start] pid for minion process is: " + minionPid);
            minionTaskDeleter.startDeletionThread();
        } catch (Exception ex) {
            log.warn("Exception during startup: " + ex, ex);
            }
    }

    public File getRootDir() {
        return this.rootDir;
    }

    public void setDiskReadOnly(boolean disk_read_only) {
        this.diskReadOnly = disk_read_only;
    }

    private int getJettyPort() {
        return jetty.getConnectors()[0].getLocalPort();
    }

    private void waitForJetty() throws Exception {
        long wait = JitterClock.globalTime();
        for (int i = 0; getJettyPort() <= 0 && i < 20; i++) {
            Thread.sleep(100);
        }
        wait = JitterClock.globalTime() - wait;
        if (wait > 1000 || getJettyPort() <= 0) {
            log.warn("[init] jetty to > " + wait + "ms to start.  on port " + getJettyPort());
        }
    }

    QueueingConsumer batchJobConsumer;
    private MessageConsumer batchControlConsumer;
    private MessageProducer queryControlProducer;
    private MessageProducer zkBatchControlProducer;
    private MessageProducer batchControlProducer;
    private Channel channel;
    private ZkClient zkClient;
    private ZkGroupMembership minionGroupMembership;

    private void connectToMQ() throws Exception {
        String[] routingKeys = new String[]{uuid, HostMessage.ALL_HOSTS};
        zkBatchControlProducer = new ZKMessageProducer(getZkClient());
        batchControlProducer = new RabbitMessageProducer("CSBatchControl", batchBrokerHost, Integer.valueOf(batchBrokerPort));
        queryControlProducer = new RabbitMessageProducer("CSBatchQuery", batchBrokerHost, Integer.valueOf(batchBrokerPort));

        com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
        factory.setHost(batchBrokerHost);
        factory.setPort(Integer.valueOf(batchBrokerPort));
        com.rabbitmq.client.Connection connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare("CSBatchJob", "direct");
        AMQP.Queue.DeclareOk result = channel.queueDeclare(uuid + ".batchJob", true, false, false, null);
        String queueName = result.getQueue();
        channel.queueBind(queueName, "CSBatchJob", uuid);
        channel.queueBind(queueName, "CSBatchJob", HostMessage.ALL_HOSTS);
        batchJobConsumer = new QueueingConsumer(channel);
        channel.basicConsume(queueName, false, batchJobConsumer);
        batchControlConsumer = new RabbitMessageConsumer(channel, "CSBatchControl", uuid + ".batchControl", this, routingKeys);
    }

    /**
     * attempt to reconnect up to n times then shut down
     */
    private void shutdown() {
        System.exit(1);
    }

    private void disconnectFromMQ() {
        try {
            batchControlConsumer.close();
        } catch (Exception ex)  {
            log.warn("", ex);
        }
        try {
            queryControlProducer.close();
        } catch (Exception ex)  {
            log.warn("", ex);
        }
        try {
            batchControlProducer.close();
        } catch (Exception ex)  {
            log.warn("", ex);
        }
        try {
            zkBatchControlProducer.close();
        } catch (Exception ex)  {
            log.warn("", ex);
        }
        try {
            channel.close();
        } catch (Exception ex)  {
            log.warn("", ex);
        }
    }

    public static int shell(String cmd, File directory) {
        try {
            File tmp = File.createTempFile(".minion", "shell", directory);
            try {
                Files.write(tmp, Bytes.toBytes("#!/bin/sh\n" + cmd + "\n"), false);
                int exit = Runtime.getRuntime().exec("sh " + tmp).waitFor();
                if (log.isDebugEnabled()) {
                    log.debug("[shell] (" + cmd + ") exited with " + exit);
                }
                return exit;
            } finally {
                tmp.delete();
            }
        } catch (Exception ex)  {
            log.warn("", ex);
        }
        return -1;
    }

    @VisibleForTesting
    public void insertJobKickMessage(CommandTaskKick kick) {
        minionStateLock.lock();
        try {
            for (int i = 0; i < jobQueue.size(); i++) {
                CommandTaskKick inQ = jobQueue.get(i);
                if (kick.getPriority() > inQ.getPriority()) {
                    kick.setSubmitTime(JitterClock.globalTime());
                    jobQueue.add(i, kick);
                    return;
                }
            }
            jobQueue.add(kick);
        } finally {
            minionStateLock.unlock();
        }
        writeState();
    }

    private void kickNextJob() throws Exception {
        minionStateLock.lock();
        try {
            if (jobQueue.isEmpty()) {
                return;
            }
            // Iterate over the queue, looking for a job that can run using the current resources
            for (CommandTaskKick nextKick : jobQueue) {
                // stop lower pri job to make room, if applicable
                boolean lackCap;
                capacityLock.lock();
                try {
                    lackCap = activeTaskKeys.size() >= maxActiveTasks;
                    if (lackCap) {
                        sendStatusMessage(new StatusTaskCantBegin(getUUID(), nextKick.getJobUuid(), nextKick.getNodeID()));
                        jobQueue.remove(nextKick);
                        break;
                    } else {
                        // remove this kick from the queue
                        jobQueue.remove(nextKick);
                        JobTask task = tasks.get(nextKick.key());
                        if (task == null) {
                            task = createNewTask(nextKick.getJobUuid(), nextKick.getNodeID());
                        }
                        task.setRetries(nextKick.getRetries());
                        try {
                            task.exec(nextKick, true);
                        } catch (Exception ex) {
                            log.warn("[kick] exception while trying to kick " + task.getName() + ": " + ex, ex);
                            task.sendEndStatus(JobTaskErrorCode.EXIT_SCRIPT_EXEC_ERROR);
                        }
                        writeState();
                        return;
                    }
                } finally {
                    capacityLock.unlock();
                }
            }
        } finally {
            minionStateLock.unlock();
        }
    }

    private List<JobTask> getMatchingJobs(JobMessage msg) {
        LinkedList<JobTask> match = new LinkedList<>();
        JobKey msgKey = msg.getJobKey();
        if (msgKey.getNodeNumber() == null) {
            for (Entry<String, JobTask> e : tasks.entrySet()) {
                String key = e.getKey();
                if (key.startsWith(msgKey.getJobUuid())) {
                    match.add(e.getValue());
                }
            }
        } else {
            JobTask job = tasks.get(msgKey.toString());
            if (job != null) {
                match.add(job);
            }
        }
        return match;
    }

    @Override
    public void onMessage(Serializable message) {
        try {
            handleMessage(message);
        } catch (Exception ex)  {
            log.warn("", ex);
        }
    }

    private void handleMessage(Serializable message) throws Exception {

        if (message instanceof CoreMessage) {
            CoreMessage core;
            try {
                core = (CoreMessage) message;
            } catch (Exception ex)  {
                log.warn("", ex);
                return;
            }
            switch (core.getMessageType()) {
                case STATUS_HOST_INFO:
                    log.debug("[host.status] request for {}", uuid);
                    sendHostStatus();
                    break;
                case CMD_TASK_STOP:
                    messageTaskExecutorService.execute(new CommandTaskStopRunner(core));
                    break;
                case CMD_TASK_REVERT:
                    messageTaskExecutorService.execute(new CommandTaskRevertRunner(core));
                    break;
                case CMD_TASK_DELETE:
                    messageTaskExecutorService.execute(new CommandTaskDeleteRunner(core));
                    break;
                case CMD_TASK_REPLICATE:
                    messageTaskExecutorService.execute(new CommandTaskReplicateRunner(core));
                    break;
                case CMD_TASK_PROMOTE_REPLICA:
                    // Legacy; ignore
                    break;
                case CMD_TASK_NEW:
                    messageTaskExecutorService.execute(new CommandCreateNewTask(core));
                    break;
                case CMD_TASK_DEMOTE_REPLICA:
                    // Legacy; ignore
                    break;
                case CMD_TASK_UPDATE_REPLICAS:
                    promoteDemoteTaskExecutorService.execute(new CommandTaskUpdateReplicasRunner(core));
                    break;
                case STATUS_TASK_JUMP_SHIP:
                    break;
                default:
                    log.warn("[mq.core] unhandled type = " + core.getMessageType());
                    break;
            }
        }
    }

    /**
     * import/update jobs from a given root
     */
    private int updateJobsMeta(File jobsRoot) throws IOException {
        int loaded = 0;
        File[] list = jobsRoot.isDirectory() ? jobsRoot.listFiles() : null;
        if (list == null || list.length == 0) {
            return 0;
        }
        for (File jobRoot : list) {
            loaded += updateJobMeta(jobRoot);
        }
        log.info("[import] " + loaded + " tasks from directory '" + jobsRoot + "'");
        return loaded;
    }

    /**
     * import/update job tasks given job root
     */
    private int updateJobMeta(File jobRoot) throws IOException {
        if (!(jobRoot.isDirectory() && jobRoot.exists())) {
            return 0;
        }
        int loaded = 0;
        String jobID = jobRoot.getName();
        for (File taskRoot : jobRoot.listFiles()) {
            loaded += updateTaskMeta(jobID, taskRoot) ? 1 : 0;
        }
        return loaded;
    }

    /**
     * update a single task from a task root dir
     */
    private boolean updateTaskMeta(String jobID, File taskRoot) throws IOException {
        if (!(taskRoot.isDirectory() && taskRoot.exists())) {
            return false;
        }
        Integer taskID = Numbers.parseInt(10, taskRoot.getName(), -1);
        if (taskID < 0) {
            log.warn("[task.update] invalid task root " + taskRoot);
            return false;
        }
        JobKey key = new JobKey(jobID, taskID);
        JobTask task = tasks.get(key.toString());
        if (task == null) {
            task = new JobTask();
            tasks.put(key.toString(), task);
        }
        if (task.restoreTaskState(taskRoot)) {
            log.warn("[import.task] " + key + " as " + (task.isRunning() ? "running" : "idle"));
            return true;
        } else {
            log.warn("[import.task] " + key + " failed");
            return false;
        }
    }

    private void writeState() {
        minionStateLock.lock();
        try {
            Files.write(stateFile, Bytes.toBytes(CodecJSON.encodeString(this)), false);
        } catch (IOException io)  {
            log.warn("", io);
            /* assume disk failure: set diskReadOnly=true and exit */
            this.diskHealthCheck.onFailure();
        } finally {
            minionStateLock.unlock();
        }
    }

    private HostState createHostState() {
        long time = System.currentTimeMillis();
        HostState status = new HostState(uuid);
        status.setHost(myHost);
        status.setPort(getJettyPort());
        status.setGroup(group);
        status.setTime(time);
        status.setUser(user);
        status.setPath(path);
        status.setReadOnly(readOnly);
        status.setDiskReadOnly(diskReadOnly);
        status.setUptime(time - startTime);
        capacityLock.lock();
        try {
            int availSlots = readOnly ? 0 : maxActiveTasks - activeTaskKeys.size();
            status.setAvailableTaskSlots(Math.max(0, availSlots));
        } finally {
            capacityLock.unlock();
        }
        status.setUsed(new HostCapacity(0, 0, 0, diskTotal.get() - diskFree.get()));
        status.setMax(new HostCapacity(0, 0, 0, diskTotal.get()));
        LinkedList<JobKey> running = new LinkedList<>();
        LinkedList<JobKey> replicating = new LinkedList<>();
        LinkedList<JobKey> backingUp = new LinkedList<>();
        LinkedList<JobKey> stoppedTasks = new LinkedList<>();
        LinkedList<JobKey> incompleteReplicas = new LinkedList<>();
        for (JobTask job : tasks.values()) {
            try {
                status.addJob(job.getJobKey().getJobUuid());
                if (job.isRunning()) {
                    running.add(job.getJobKey());
                } else if (job.isReplicating() && job.isProcessRunning(job.replicatePid)) {
                    replicating.add(job.getJobKey());
                } else if (job.isBackingUp()) {
                    backingUp.add(job.getJobKey());
                } else if (job.getLiveDir().exists()) {
                    if (job.isComplete()) {
                        stoppedTasks.add(job.getJobKey());
                    } else {
                        incompleteReplicas.add(job.getJobKey());
                    }
                }
            } catch (Exception ex) {
                log.warn("Failed to detect status of job " + job + "; omitting from host state", ex);
            }

        }
        status.setRunning(running.toArray(new JobKey[running.size()]));
        status.setReplicating(replicating.toArray(new JobKey[replicating.size()]));
        status.setBackingUp(backingUp.toArray(new JobKey[backingUp.size()]));
        status.setStopped(stoppedTasks.toArray(new JobKey[stoppedTasks.size()]));
        status.setIncompleteReplicas(incompleteReplicas.toArray(new JobKey[incompleteReplicas.size()]));
        LinkedList<JobKey> queued = new LinkedList<JobKey>();
        minionStateLock.lock();
        try {
            for (CommandTaskKick kick : jobQueue) {
                queued.add(kick.getJobKey());
            }
        } finally {
            minionStateLock.unlock();
        }
        status.setQueued(queued.toArray(new JobKey[queued.size()]));
        status.setMeanActiveTasks(activeTaskHistogram.mean());
        status.setMinionTypes(minionTypes);
        status.setUpdated();
        return status;
    }

    public void sendHostStatus() {
        updateHostConfig(createHostState());
    }

    public String getUUID() {
        return uuid;
    }

    // Not Thread Safe!
    private ZkClient getZkClient() {
        if (zkClient == null) {
            zkClient = ZkClientFactory.makeStandardClient();
            zkClient.subscribeStateChanges(new SessionExpireListener(this));
        }
        return zkClient;
    }

    @VisibleForTesting
    protected void closeZkClient() {
        if (zkClient != null) {
            zkClient.close();
        }
    }

    @Override
    public void handleExpiredSession() {
        joinGroup();
    }

    protected void joinGroup() {
        minionGroupMembership = new ZkGroupMembership(getZkClient());
        ZkHelpers.makeSurePersistentPathExists(getZkClient(), ("/minion/up"));
        log.info("joining group: " + "/minion/up");
        minionGroupMembership.addToGroup("/minion/up", getUUID(), shutdown);
    }


    private void sendControlMessage(HostMessage msg) {
        synchronized (jmsxmitlock) {
            try {
                if (batchControlProducer != null) {
                    batchControlProducer.sendMessage(msg, msg.getHostUuid());
                }
            } catch (Exception ex) {
                log.warn("[mq.ctrl.send] fail with " + ex, ex);
                shutdown();
            }
        }
    }

    private void sendStatusMessage(HostMessage msg) {
        synchronized (jmsxmitlock) {
            try {
                if (batchControlProducer != null) {
                    batchControlProducer.sendMessage(msg, "SPAWN");
                }
            } catch (Exception ex) {
                log.warn("[mq.ctrl.send] fail with " + ex, ex);
                shutdown();
            }
        }
    }


    private void updateHostConfig(HostMessage msg) {
        boolean sent = false;
        for (int i = 0; i < sendStatusRetries; i++) {
            synchronized (jmsxmitlock) {
                try {
                    if (shutdown.get()) {
                        return; // Interrupt any existing status updates; we'll send one during the shutdown event anyway
                    }
                    if (zkBatchControlProducer != null) {
                        // TODO: move to /minion/state/ or some other dir
                        zkBatchControlProducer.sendMessage(msg, MINION_ZK_PATH + uuid);
                    }
                    sent = true;
                    break;
                } catch (Exception ex) {
                    log.warn("[mq.ctrl.send] exception " + ex, ex);
                    if (i < sendStatusRetries - 1) {
                        log.warn("[mq.ctrl.send] fail on try " + i + "; retrying");
                    }
                    sendStatusFailCount.inc();
                }
            }
            try {
                Thread.sleep(sendStatusRetryDelay);
            } catch (InterruptedException ie) {
                log.warn("[mq.ctrl.send] interrupted during retry delay");
            }
        }
        if (!sent) {
            sendStatusFailAfterRetriesCount.inc();
            log.warn("[mq.ctrl.send] fail after retrying");
            shutdown();
        }
    }

    @Override
    public void doStop() {
        shutdown.set(true);
        writeState();
        log.info("[minion] stopping and sending updated stats to spawn");
        sendHostStatus();
        if (runner != null) {
            runner.shutdown();
        }
        try {
            Thread.sleep(1000);
        } catch (Exception ex)  {
            log.warn("", ex);
        }
        disconnectFromMQ();
    }

    @Override
    public void doStart() {
    }

    private synchronized int findNextPort() {
        if (minJobPort == 0) {
            return 0;
        }
        int startPort = nextPort;
        while (true) {
            if (nextPort > maxJobPort) {
                nextPort = minJobPort;
            }
            try {
                ServerSocket ss = new ServerSocket(nextPort++);
//              ss.setReuseAddress(true);
                ss.close();
                break;
            } catch (Exception ex) {
                if (startPort == nextPort) {
                    throw new RuntimeException("unable to find any free ports");
                }
                log.warn("[nextport] skipping port " + nextPort + " due to " + ex);
            }
        }
        return nextPort;
    }

    @SuppressWarnings("serial")
    private static class ExecException extends Exception {

        ExecException(String msg) {
            super(msg);
        }
    }

    private String getTaskBaseDir(String baseDir, String id, int node) {
        return new StringBuilder().append(baseDir).append("/").append(id).append("/").append(node).toString();
    }

    private class TaskRunner extends Thread {

        private boolean done;

        public void shutdown() {
            done = true;
            interrupt();
        }

        public void run() {
            while (!done) {
                QueueingConsumer.Delivery delivery = null;
                try {
                    delivery = batchJobConsumer.nextDelivery();
                    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(delivery.getBody()));
                    HostMessage hostMessage = (HostMessage) ois.readObject();
                    if (hostMessage.getMessageType() != CoreMessage.TYPE.CMD_TASK_KICK) {
                        log.warn("[task.runner] unknown command type : " + hostMessage.getMessageType());
                        continue;
                    }
                    CommandTaskKick kick = (CommandTaskKick) hostMessage;
                    insertJobKickMessage(kick);
                    kickNextJob();
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                } catch (InterruptedException ex) {
                    exitTaskRunner(ex);
                } catch (Throwable ex) {
                    try {
                        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    } catch (Exception e) {
                        log.warn("[task.runner] unable to nack message delivery due to " + e, ex);
                        }
                    log.warn("[task.runner] error: " + ex);
                    if (!(ex instanceof ExecException)) {
                        ex.printStackTrace();
                    }
                    // backoff to prevent excessive message production when rabbitmq is down
                    try {
                        jobConsumerBackoff.sleep();
                    } catch (InterruptedException e) {
                        exitTaskRunner(e);
                    }
                }
            }
        }

        private void exitTaskRunner(InterruptedException ex) {
            if (!done) {
                log.warn("", ex);
            } else {
                log.warn("[task.runner] exiting");
            }
        }
    }

    public static class FileWatcher {

        private File file;
        private long lastSize;
        private long lastEnd;
        private RandomAccessFile access;
        private String needle;

        FileWatcher(File file, String needle) {
            try {
                this.access = new RandomAccessFile(file, "r");
                this.needle = needle;
                this.file = file;
            } catch (Exception ex)  {
                log.warn("", ex);
            }
        }

        public boolean containsKill() {
            log.warn("containsKill(" + file + "," + access + ")");
            if (access != null) {
                try {
                    long len = file.length();
                    if (len > lastSize) {
                        lastSize = len;
                        // find last CR
                        log.warn("searching " + lastEnd + " to " + len);
                        for (long pos = len - 1; pos >= lastEnd; pos--) {
                            access.seek(pos);
                            if (access.read() == '\n') {
                                long start = lastEnd;
                                long size = pos - start;
                                if (size > 32768) {
                                    log.warn("[warning] skipping search @ " + size);
                                    lastEnd = pos;
                                    return false;
                                } else if (size > 4096) {
                                    log.warn("[warning] searching > 4k space @ " + size);
                                }
                                byte scan[] = new byte[(int) size];
                                access.seek(start);
                                access.readFully(scan);
                                log.warn("scan of " + Bytes.toString(scan));
                                lastEnd = pos;
                                return (Bytes.toString(scan).indexOf(needle) >= 0);
                            }
                        }
                    }
                } catch (Exception ex)  {
                    log.warn("", ex);
                }
            }
            return false;
        }
    }

    public Integer getPID(File pidFile) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("[getpid] " + pidFile + " --> " + (pidFile != null ? pidFile.exists() : "<null>"));
            }
            return (pidFile != null && pidFile.exists()) ? Integer.parseInt(Bytes.toString(Files.read(pidFile)).trim()) : null;
        } catch (Exception ex)  {
            log.warn("", ex);
            return null;
        }
    }

    private String execCommandReturnStdOut(String cmd) throws InterruptedException, IOException {
        SimpleExec command = new SimpleExec(cmd).join();
        return command.stdoutString();
    }

    // Sorry Illumos port...
    public String getCmdLine(int pid) {
        try {
            String cmd;
            if (useMacFriendlyPSCommands) {
                cmd = execCommandReturnStdOut("ps -f " + pid);
            } else {
                File cmdFile = new File("/proc/" + pid + "/cmdline");
                cmd = Bytes.toString(Files.read(cmdFile)).trim().replace('\0', ' ');
            }
            log.warn("found cmd " + cmd + "  for pid: " + pid);
            return cmd;

        } catch (Exception ex) {
            log.warn("error searching for pidfile for pid: " + pid, ex);
            return null;
        }
    }


    /**
     * for tracking state
     */
    public class JobTask implements Codec.Codable {

        @Codec.Set(codable = true, required = true)
        private String id;
        @Codec.Set(codable = true, required = true)
        private Integer node;
        @Codec.Set(codable = true)
        private Integer nodeCount;
        @Codec.Set(codable = true)
        private CommandTaskKick kick;
        @Codec.Set(codable = true, required = true)
        private int runCount;
        @Codec.Set(codable = true, required = true)
        private long runTime;
        @Codec.Set(codable = true)
        private long startTime;
        @Codec.Set(codable = true)
        private boolean monitored = true;
        @Codec.Set(codable = true)
        private long fileCount;
        @Codec.Set(codable = true)
        private long fileBytes;
        @Codec.Set(codable = true)
        private volatile boolean deleted;
        @Codec.Set(codable = true)
        private int retries;

        private volatile ReplicaTarget[] failureRecoveryReplicas;
        private volatile ReplicaTarget[] replicas;

        @Codec.Set(codable = true)
        private long replicateStartTime;
        @Codec.Set(codable = true)
        private long backupStartTime;

        private Process process;
        private Thread workItemThread;
        private File taskRoot;
        private File jobRun;
        private File replicateSH;
        private File replicateRun;
        private File backupSH;
        private File backupRun;
        private File jobDone;
        private File replicateDone;
        private File backupDone;
        private File jobBackup;
        private File jobStopped;
        private File jobDir;
        private File logOut;
        private File logErr;
        private File jobPid;
        private File replicatePid;
        private File backupPid;
        private File jobPort;
        private Integer port;

        public void setDeleted(boolean deleted) {
            this.deleted = deleted;
        }

        public JobKey getJobKey() {
            return new JobKey(id, node);
        }

        public CommandTaskKick getKick() {
            return kick;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getReplicateStartTime() {
            return replicateStartTime;
        }

        public void setReplicateStartTime(long replicateStartTime) {
            this.replicateStartTime = replicateStartTime;
        }

        public long getBackupStartTime() {
            return backupStartTime;
        }

        public void setBackupStartTime(long backupStartTime) {
            this.backupStartTime = backupStartTime;
        }

        public void setProcess(Process p) {
            this.process = p;
        }

        public void interruptProcess() {
            if (this.process != null) {
                this.process.destroy();
            }
        }

        public int getRetries() {
            return retries;
        }

        public void setRetries(int retries) {
            this.retries = retries;
        }

        public void clearFailureReplicas() {
            // Merge all failureRecoveryReplicas into the master list, then clear failureRecoveryReplicas
            List<ReplicaTarget> finalReplicas = replicas != null ? new ArrayList<>(Arrays.asList(replicas)) : new ArrayList<ReplicaTarget>();
            if (failureRecoveryReplicas != null) {
                finalReplicas.addAll(Arrays.asList(failureRecoveryReplicas));
            }
            replicas = finalReplicas.toArray(new ReplicaTarget[finalReplicas.size()]);
            failureRecoveryReplicas = new ReplicaTarget[]{};
        }

        public boolean isDeleted() {
            return deleted;
        }

        public void setWorkItemThread(MinionWorkItem workItemThread) {
            this.workItemThread = workItemThread == null ? null : new Thread(workItemThread);
        }

        public void save() {
            try {
                Files.write(new File(getConfigDir(), "job.state"), Bytes.toBytes(CodecJSON.encodeString(this, 4)), false);
            } catch (Exception e)  {
                log.warn("", e);
            }
        }

        private void monitor() {
            monitored = true;
            save();
        }

        public void unmonitor() {
            monitored = false;
            save();
        }

        public void updateFileStats() {
            final TimerContext updateTimer = fileStatsTimer.time();
            FileStats stats = new FileStats();
            stats.update(jobDir);
            try {
                Files.write(new File(getConfigDir(), "job.stats"), Bytes.toBytes(CodecJSON.encodeString(stats)), false);
            } catch (Exception e)  {
                log.warn("", e);
            }
            fileCount = stats.count;
            fileBytes = stats.bytes;
            updateTimer.stop();
        }

        public void allocate() {
            capacityLock.lock();
            try {
                activeTaskKeys.add(this.getName());
            } finally {
                capacityLock.unlock();
            }
        }

        public void deallocate() {
            capacityLock.lock();
            try {
                activeTaskKeys.remove(this.getName());
            } finally {
                capacityLock.unlock();
            }
        }

        public void sendNewStatusToReplicaHost(String hostUUID) {
            sendControlMessage(new CommandTaskNew(hostUUID, getJobKey().getJobUuid(), getJobKey().getNodeNumber()));
        }

        public void sendEndStatus(int exit) {
            sendEndStatus(exit, null);
        }

        public void sendEndStatus(int exit, String choreWatcherKey) {
            TaskExitState exitState = new TaskExitState();
            File jobExit = new File(jobDir, "job.exit");
            if (jobExit.exists() && jobExit.canRead()) {
                try {
                    new CodecJSON().decode(exitState, Files.read(jobExit));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            exitState.setWasStopped(wasStopped());
            sendStatusMessage(new StatusTaskEnd(uuid, id, node, exit, fileCount, fileBytes).
                    setChoreWatcherKey(choreWatcherKey).
                    setExitState(exitState));
            try {
                kickNextJob();
            } catch (Exception e) {
                log.warn("[task.kick] exception while trying to kick next job: " + e, e);
                }
        }

        public void sendPort() {
            sendStatusMessage(new StatusTaskPort(uuid, kick.getJobUuid(), kick.getNodeID(), port));
        }

        /* restore a job state from a job/task-id root directory */
        private boolean restoreTaskState(File taskDir) throws IOException {
            taskRoot = taskDir;
            File liveDir = new File(taskDir, "live");
            File replicaDir = new File(taskDir, "replica");
            File configDir = new File(taskDir, "config");
            Files.initDirectory(configDir);
            String jobID = taskDir.getParentFile().getName();
            String nodeID = taskDir.getName();
            String taskPath = jobID + "/" + nodeID;
            if (replicaDir.isDirectory()) {
                // Cut over replica to live
                replicaDir.renameTo(liveDir);
            } else if (liveDir.exists() && !liveEverywhereMarkerFile.exists()) {
                // On first startup, mark any existing "live" directory as complete.
                new File(liveDir, "replicate.complete").createNewFile();
            }
            if (!liveDir.isDirectory()) {
                log.warn("[restore] " + taskPath + " has no live or replica directories");
                return false;
            }
            id = jobID;
            node = Integer.parseInt(nodeID);
            initializeFileVariables();
            if (!liveEverywhereMarkerFile.exists()) {
                // On first startup, make sure to get to known idle state
                jobDone.createNewFile();
                backupDone.createNewFile();
                replicateDone.createNewFile();
            }
            File jobState = new File(configDir, "job.state");
            if (jobState.exists()) {
                try {
                    CodecJSON.decodeString(this, Bytes.toString(Files.read(jobState)));
                } catch (Exception e)  {
                    log.warn("", e);
                    return false;
                }
            }
            if (Integer.parseInt(nodeID) != node) {
                log.warn("[restore] " + taskPath + " mismatch with node # " + node);
                return false;
            }
            if (!jobID.equals(id)) {
                log.warn("[restore] " + taskPath + " mismatch with node id " + id);
                return false;
            }
            monitored = true;
            recoverWorkItem();
            return true;
        }

        /* If minion detects that a task was running when the minion was shut down, attempt to recover by looking for the pid */
        private void recoverWorkItem() {
            try {
                if (isRunning()) {
                    log.warn("[restore] " + getName() + " as running");
                    exec(this.kick, false);
                } else if (isReplicating()) {
                    log.warn("[restore] " + getName() + " as replicating");
                    execReplicate(null, false, false);
                } else if (isBackingUp()) {
                    log.warn("[restore] " + getName() + " as backing up");
                    execBackup(null, false);
                } else if ((startTime > 0 || replicateStartTime > 0 || backupStartTime > 0)) {
                    // Minion had a process running that finished during the downtime; notify Spawn
                    log.warn("[restore]" + getName() + " as previously active; now finished");
                    startTime = 0;
                    replicateStartTime = 0;
                    backupStartTime = 0;
                    sendEndStatus(0);
                }
            } catch (Exception ex) {
                log.warn("WARNING: failed to restore state for " + getName() + ": " + ex, ex);
                }

        }

        private void initializeFileVariables() {
            jobDir = getLiveDir();
            File configDir = getConfigDir();
            File logRoot = new File(jobDir, "log");
            logOut = new File(logRoot, "log.out");
            logErr = new File(logRoot, "log.err");
            jobPid = new File(configDir, "job.pid");
            replicatePid = new File(configDir, "replicate.pid");
            backupPid = new File(configDir, "backup.pid");
            jobPort = new File(jobDir, "job.port");
            jobDone = new File(configDir, "job.done");
            replicateDone = new File(configDir, "replicate.done");
            backupDone = new File(configDir, "backup.done");
        }

        private boolean isComplete() {
            File replicaComplete = new File(getLiveDir(), "replicate.complete");
            return replicaComplete.exists();
        }

        private boolean shouldExecuteReplica(ReplicaTarget replica) {
            if (replica.getHostUuid().equals(uuid)) {
                log.warn("Host: " + uuid + " received a replication target of itself, this is NOT allowed for " + getName());
                return false;
            }
            return true;
        }

        private List<String> assembleReplicateCommandAndInformSpawn(ReplicaTarget replica, boolean replicateAllBackups) throws IOException {
            List<String> rv = new ArrayList<String>();
            if (replica == null || !shouldExecuteReplica(replica)) {
                return null;
            }
            try {
                String target = getTaskBaseDir(replica.getBaseDir(), id, node);
                if (!replicateAllBackups) {
                    target += "/live";
                }
                String userAT = replica.getUserAT();
                String mkTarget = remoteConnectMethod + " " + userAT + " mkdir -p " + target + "/";
                log.warn("[replicate] " + getJobKey() + " to " + userAT + ":" + target);
                if (log.isDebugEnabled()) {
                    log.debug(" --> " + mkTarget);
                }
                int runCount = kick != null ? kick.getRunCount() : 0;
                sendStatusMessage(new StatusTaskReplica(replica.getHostUuid(), id, node, runCount, System.currentTimeMillis()));
                rv.add(mkTarget);
                if (replicateAllBackups) {
                    rv.add(createRsyncCommand(userAT, jobDir.getParentFile().getAbsolutePath() + "/", target) +
                           " \n" + createTouchCommand(false, userAT, target + "/live/replicate.complete")
                    );
                } else {
                    rv.add(createDeleteCommand(false, userAT, target + "/replicate.complete") +
                           "\n" + createRsyncCommand(userAT, jobDir.getAbsolutePath() + "/", target) +
                           "\n" + createTouchCommand(false, userAT, target + "/replicate.complete")
                    );
                }
            } catch (Exception ex) {
                log.warn("failed to replicate " + this.getJobKey() + " to " + replica.getHost(), ex);
            }
            return rv;
        }

        private List<String> assembleBackupCommandsForHost(boolean local, ReplicaTarget replica, List<String> symlinkCommands, List<String> deleteCommands) {
            List<String> copyCommands = new ArrayList<String>();
            for (ScheduledBackupType type : ScheduledBackupType.getBackupTypes().values()) {
                String[] allBackups = local ? findLocalBackups(false) : findRemoteBackups(false, replica);
                String[] validBackups = allBackups;
                String backupName = type.generateCurrentName(true);
                String symlinkName = type.getSymlinkName();
                String userAT = local ? null : replica.getUserAT();
                String source = "live";
                String path = local ? jobDir.getParentFile().getAbsolutePath() : getTaskBaseDir(replica.getBaseDir(), id, node);
                int maxNumBackups = getMaxNumBackupsForType(type);
                if (maxNumBackups > 0 && type.shouldMakeNewBackup(validBackups)) {
                    String backupCMD = createBackupCommand(local, userAT, path, source, backupName);
                    copyCommands.add(backupCMD);
                    if (symlinkName != null) {
                        symlinkCommands.add(createSymlinkCommand(local, userAT, path, backupName, symlinkName));
                    }
                    maxNumBackups -= 1; // Diminish the max number by one, because we're about to add a new one
                }
                List<String> backupsToDelete = type.oldBackupsToDelete(allBackups, validBackups, maxNumBackups);
                for (String oldBackup : backupsToDelete) {
                    if (MinionTaskDeleter.shouldDeleteBackup(oldBackup, type)) {
                        deleteCommands.add(createDeleteCommand(local, userAT, path + "/" + oldBackup));
                    }
                }
            }
            writeState();
            return copyCommands;
        }

        private String createRsyncCommand(String userAT, String source, String target) throws Exception {
            return "retry " + rsyncCommand + " -Hqav --exclude config --exclude gold --exclude replicate.complete --delete-after -e \\'" + remoteConnectMethod + "\\' " + source + " " + userAT + ":" + target;
        }

        private String createBackupCommand(boolean local, String userAT, String baseDir, String source, String name) {
            String sourceDir = baseDir + "/" + source;
            String targetDir = baseDir + "/" + name;
            log.warn("[backup] executing backup from " + sourceDir + " to " + targetDir);
            return createDeleteCommand(local, userAT, targetDir) + " && " +
                   createCopyCommand(local, userAT, sourceDir, targetDir) + " && " +
                   createTouchCommand(local, userAT, targetDir + "/backup.complete");
        }

        private String createSymlinkCommand(boolean local, String userAt, String baseDir, String source, String name) {
            String linkDir = baseDir + "/" + name;
            String tmpName = linkDir + "_tmp";
            return wrapCommandWithRetries(local, userAt, "if [ ! -L " + linkDir + " ]; then rm -rf " + linkDir + " ; fi && " + lncmd + " -nsf " + source + " " + tmpName + " && " + mvcmd + " -Tf " + tmpName + " " + linkDir);
        }

        private String createCopyCommand(boolean local, String userAt, String sourceDir, String targetDir) {
            String cpParams = linkBackup ? " -lr " : " -r ";
            return wrapCommandWithRetries(local, userAt, cpcmd + cpParams + sourceDir + " " + targetDir);
        }

        private String createTouchCommand(boolean local, String userAT, String path) {
            return wrapCommandWithRetries(local, userAT, "touch " + path);
        }

        private String createDeleteCommand(boolean local, String userAT, String dirPath) {
            return wrapCommandWithRetries(local, userAT, rmcmd + " -rf " + dirPath);
        }

        private String wrapCommandWithRetries(boolean local, String userAt, String command) {
            return "retry \"" + (local ? "" : remoteConnectMethod + " " + userAt + " '") + command + (local ? "" : "'") + "\"";
        }

        /**
         * Find local backups for a task.
         *
         * @param completeOnly Whether to restrict to backups that contain the backup.complete file
         * @return A list of directory names
         */
        private String[] findLocalBackups(boolean completeOnly) {
            File[] dirs = jobDir.getParentFile().listFiles();
            if (dirs == null) {
                return new String[]{};
            }
            List<String> rvList = new ArrayList<>();
            for (File dir : dirs) {
                if (dir.isDirectory()) {
                    if (!completeOnly || Strings.contains(dir.list(), "backup.complete")) {
                        rvList.add(dir.getName());
                    }
                }
            }
            Collections.sort(rvList);
            return rvList.toArray(new String[]{});
        }

        /**
         * Find backups for a task on a replica host
         *
         * @param completeOnly Whether to restrict to backups that contain the backup.complete file
         * @param replica      The ReplicaTarget object describing the destination for this replica
         * @return A list of directory names
         */
        private String[] findRemoteBackups(boolean completeOnly, ReplicaTarget replica) {
            try {
                String userAT = replica.getUser() + "@" + replica.getHost();
                String baseDir = getTaskBaseDir(replica.getBaseDir(), id, node);
                if (completeOnly) {
                    baseDir += "/*/backup.complete";
                }
                String lsResult = execCommandReturnStdOut(remoteConnectMethod + " " + userAT + " " + lscmd + " " + baseDir);
                String[] lines = lsResult.split("\n");
                if (completeOnly) {
                    List<String> rv = new ArrayList<String>(lines.length);
                    for (String line : lines) {
                        String[] splitLine = line.split("/");
                        if (splitLine.length > 2) {
                            rv.add(splitLine[splitLine.length - 2]);
                        }
                    }
                    return rv.toArray(new String[]{});
                } else {
                    return lines;
                }
            } catch (Exception ex) {
                return new String[]{};
            }
        }

        private String execCommandReturnStdOut(String sshCMD) throws InterruptedException, IOException {
            String[] wrappedCMD = new String[]{"/bin/sh", "-c", sshCMD};
            SimpleExec command = runCommand(wrappedCMD, null);
            if (command.exitCode() == 0) {
                return command.stdoutString();
            } else {
                return "";
            }
        }

        private SimpleExec runCommand(String[] sshCMDArray, String sshCMD) throws InterruptedException, IOException {
            SimpleExec command;
            if (sshCMD != null) {
                command = new SimpleExec(sshCMD).join();
            } else {
                command = new SimpleExec(sshCMDArray).join();
            }
            return command;
        }

        /* Read the proper number of backups for each type from the kick parameters */
        private int getMaxNumBackupsForType(ScheduledBackupType type) {
            if (type instanceof GoldBackup) {
                return 3; // Keep 3 gold backups around so that these directories will linger for query/streaming stability
            }
            if (kick == null) {
                return -1; // If we're not sure how many backups to create, hold off until we receive a task kick
            }
            if (type instanceof HourlyBackup) {
                return kick.getHourlyBackups();
            } else if (type instanceof DailyBackup) {
                return kick.getDailyBackups();
            } else if (type instanceof WeeklyBackup) {
                return kick.getWeeklyBackups();
            } else if (type instanceof MonthlyBackup) {
                return kick.getMonthlyBackups();
            } else {
                return 0; // Unknown backup type
            }
        }

        /**
         * Move the specified backup dir onto the live dir
         *
         * @param backupDir The "good" version of a task
         * @param targetDir The target directory, generally "live", which may have bad/incomplete data
         * @return True if the operation succeeds
         */
        public boolean promoteBackupToLive(File backupDir, File targetDir) {
            if (targetDir != null && backupDir != null && backupDir.exists() && backupDir.isDirectory()) {
                moveAndDeleteAsync(targetDir);
                // Copy the backup directory onto the target directory
                String cpCMD = cpcmd + (linkBackup ? " -lrf " : " -rf ");
                return shell(cpCMD + backupDir + " " + targetDir + " >> /dev/null 2>&1", rootDir) == 0;
            } else {
                log.warn("[restore] invalid backup dir " + backupDir);
            }
            return false;
        }

        /**
         * Move a file to a temporary location, then delete it asynchronously via a request to MinionTaskDeleter
         *
         * @param file The file to be deleted.
         */
        private void moveAndDeleteAsync(File file) {
            if (file != null && file.exists()) {
                File tmpLocation = new File(file.getParent(), "BAD-" + System.currentTimeMillis());
                if (file.renameTo(tmpLocation)) {
                    minionTaskDeleter.submitPathToDelete(tmpLocation.getPath());
                } else {
                    throw new RuntimeException("Could not rename file for asynchronous deletion: " + file);
                }
            }
        }

        public boolean revertToBackup(int revision, long time, String type) {
            revertLock.lock();
            try {
                if (isRunning() || isReplicating() || isBackingUp()) {
                    log.warn("[revert] cannot promote backup for active task " + getName());
                    return false;
                }
                ScheduledBackupType typeToUse = ScheduledBackupType.getBackupTypes().get(type);
                if (typeToUse == null) {
                    log.warn("[revert] unrecognized backup type " + type);
                    return false;
                }
                String backupName;
                if (revision < 0) {
                    backupName = getBackupByTime(time, type);
                } else {
                    backupName = getBackupByRevision(revision, type);
                }
                if (backupName == null) {
                    log.warn("[revert] found no backups of type " + type + " and time " + time + " to revert to for " + getName() + "; failing");
                    return false;
                }
                File oldBackup = new File(jobDir.getParentFile(), backupName);
                log.warn("[revert] " + getName() + " from " + oldBackup);
                sendStatusMessage(new StatusTaskRevert(getUUID(), id, node));
                boolean promoteSuccess = promoteBackupToLive(oldBackup, jobDir);
                if (promoteSuccess) {
                    try {
                        execReplicate(null, false, true);
                        return true;
                    } catch (Exception ex) {
                        log.warn("[revert] post-revert replicate of " + getName() + " failed with exception " + ex, ex);
                        return false;
                    }
                } else {
                    log.warn("[revert] " + getName() + " from " + oldBackup + " failed");
                    sendEndStatus(JobTaskErrorCode.EXIT_REVERT_FAILURE);
                    return false;
                }
            } finally {
                revertLock.unlock();
            }
        }

        private String getBackupByTime(long time, String type) {
            ScheduledBackupType backupType = ScheduledBackupType.getBackupTypes().get(type);
            String[] backups = findLocalBackups(true);
            if (backups == null || backups.length == 0) {
                log.warn("[revert] fail, there are no local backups of type " + type + " for " + getName());
                return null;
            }
            String timeName = backupType.stripSuffixAndPrefix(backupType.generateNameForTime(time, true));
            for (String backupName : backups) {
                if (backupType.isValidName(backupName) && (backupType.stripSuffixAndPrefix(backupName).equals(timeName))) {
                    return backupName;
                }
            }
            log.warn("[revert] fail, invalid backup time for " + getName() + ": " + time);
            return null;
        }

        /**
         * Get all complete backups, ordered from most recent to earliest.
         * @return A list of backup names
         */
        public List<String> getBackupsOrdered() {
            List<String> backups = new ArrayList<> (Arrays.asList(findLocalBackups(true)));
            ScheduledBackupType.sortBackupsByTime(backups);
            return backups;
        }

        /**
         * Fetch the name of the backup directory for this task, n revisions back
         * @param revision How far to go back -- 0 for latest stable version, 1 for the next oldest, etc.
         * @param type Which backup type to use.
         * @return The name of the appropriate complete backup, if found, and null if no such backup was found
         */
        private String getBackupByRevision(int revision, String type) {

            String[] backupsRaw = findLocalBackups(true);
            List<String> backups = new ArrayList<>();
            if (backupsRaw == null) {
                return null;
            }
            if ("all".equals(type)) {
                backups.addAll(Arrays.asList(backupsRaw));
                ScheduledBackupType.sortBackupsByTime(backups);
            } else {
                ScheduledBackupType backupType = ScheduledBackupType.getBackupTypes().get(type);
                for (String backup : backupsRaw) {
                    if (backupType.isValidName(backup)) {
                        backups.add(backup);
                    }
                }
            }
            int offset = (backups.size() - 1 - revision);
            if (revision < 0 || offset < 0 || offset >= backups.size()) {
                log.warn("[revert] fail: can't find revision=" + revision + " with only " + backups.size() + " complete backups");
                return null;
            }
            return backups.get(offset);
        }

        private void require(boolean test, String msg) throws ExecException {
            if (!test) {
                throw new ExecException(msg);
            }
        }

        private void requireNewOrEqual(Object currentValue, Object newValue, String valueName) throws IllegalArgumentException {
            if (currentValue != null && !currentValue.equals(newValue)) {
                throw new IllegalArgumentException("value mismatch for '" + valueName + "' " + newValue + " != " + currentValue);
            }
        }

        public void exec(CommandTaskKick kickMessage, boolean execute) throws Exception {
            // setup data directory
            jobDir = Files.initDirectory(new File(rootDir, id + File.separator + node + File.separator + "live"));
            File configDir = getConfigDir();
            if (!configDir.exists()) {
                Files.initDirectory(configDir);
            }
            File logDir = new File(jobDir, "log");
            Files.initDirectory(logDir);
            replicateDone = new File(configDir, "replicate.done");
            jobRun = new File(configDir, "job.run");
            jobDone = new File(configDir, "job.done");
            logOut = new File(logDir, "log.out");
            logErr = new File(logDir, "log.err");
            jobPid = new File(configDir, "job.pid");
            jobPort = new File(jobDir, "job.port");
            jobStopped = new File(jobDir, "job.stopped");
            if (execute) {
                File replicateComplete = new File(getLiveDir(), "replicate.complete");
                replicateComplete.createNewFile();
                replicas = kickMessage.getReplicas();
                String jobId = kickMessage.getJobUuid();
                int jobNode = kickMessage.getJobKey().getNodeNumber();
                if (log.isDebugEnabled()) {
                    log.debug("[task.exec] " + kickMessage.getJobKey());
                }
                require(testTaskIdle(), "task is not idle");
                String jobCommand = kickMessage.getCommand();
                require(!Strings.isEmpty(jobCommand), "task command is missing or empty");
                // ensure we're not changing something critical on a re-spawn
                int jobNodes = kickMessage.getJobNodes();
                requireNewOrEqual(id, jobId, "Job ID");
                requireNewOrEqual(node, jobNode, "Job Node");
                requireNewOrEqual(nodeCount, jobNodes, "Job Node Count");
                // store the new values
                id = jobId;
                node = jobNode;
                nodeCount = jobNodes;
                kick = kickMessage;
                retries = kick != null ? kick.getRetries() : 0;
                // allocate type slot if applicable
                sendStatusMessage(new StatusTaskBegin(uuid, id, node));
                // store in jobs on first run
                if (runCount == 0) {
                    log.warn("[task.exec] first time running " + getName());
                }
                String jobConfig = kickMessage.getConfig();
                if (jobConfig != null) {
                    Files.write(new File(jobDir, "job.conf"), Bytes.toBytes(jobConfig), false);
                }
                // create exec command
                jobCommand = jobCommand.replace("{{jobdir}}", jobDir.getPath()).replace("{{jobid}}", jobId).replace("{{port}}", findNextPort() + "").replace("{{node}}", jobNode + "").replace(
                        "{{nodes}}", jobNodes + "");
                log.warn("[task.exec] starting " + jobDir.getPath() + " with retries=" + retries);
                // create shell wrapper
                require(deleteFiles(jobPid, jobPort, jobDone, jobStopped), "failed to delete files");
                port = null;
                String stamp = timeFormat.print(System.currentTimeMillis());
                File logOutTmp = new File(logDir, "log-" + stamp + ".out");
                File logErrTmp = new File(logDir, "log-" + stamp + ".err");
                StringBuilder bash = new StringBuilder("#!/bin/bash\n");
                bash.append("find " + logDir + " -type f -mtime +30 -exec rm {} \\;\n");
                bash.append("rm -f " + logOut + " " + logErr + "\n");
                bash.append("ln -s " + logOutTmp.getName() + " " + logOut + "\n");
                bash.append("ln -s " + logErrTmp.getName() + " " + logErr + "\n");
                bash.append("(\n");
                bash.append("cd " + jobDir + "\n");
                bash.append("(" + jobCommand + ") &\n");
                bash.append("pid=$!\n");
                bash.append("echo ${pid} > " + jobPid.getCanonicalPath() + "\n");
                bash.append("exit=0\n");
                bash.append("wait ${pid} || exit=$?\n");
                bash.append("echo ${exit} > " + jobDone.getCanonicalPath() + "\n");
                bash.append("exit ${exit}\n");
                bash.append(") >" + logOutTmp + " 2>" + logErrTmp + " &\n");
                Files.write(jobRun, Bytes.toBytes(bash.toString()), false);
                runCount++;
            }
            this.startTime = System.currentTimeMillis();
            // save it
            save();
            sendHostStatus();
            // mark it active
            capacityLock.lock();
            try {
                activeTaskKeys.add(getName());
            } finally {
                capacityLock.unlock();
            }
            // start watcher, which will fire it up
            workItemThread = new Thread(new RunTaskWorkItem(jobDir, jobPid, jobRun, jobDone, this, execute, retries));
            workItemThread.setName("RunTask-WorkItem-" + getName());
            workItemThread.start();
        }

        public void execReplicate(String choreWatcherKey, boolean replicateAllBackups, boolean execute) throws Exception {
            if (log.isDebugEnabled()) {
                log.debug("[task.execReplicate] " + this.getJobKey());
            }
            require(testTaskIdle(), "task is not idle");
            if ((replicas == null || replicas.length == 0) && (failureRecoveryReplicas == null || failureRecoveryReplicas.length == 0)) {
                execBackup(choreWatcherKey, true);
                return;
            }
            if (findActiveRsync(id, node) != null) {
                String msg = "Replicate failed because an existing rsync process was found for " + getName();
                log.warn("[task.execReplicate] " + msg);
                sendEndStatus(JobTaskErrorCode.EXIT_REPLICATE_FAILURE);
                shell(echoWithDate_cmd + msg + " >> " + logErr.getCanonicalPath(), rootDir);
                return;
            }
            sendStatusMessage(new StatusTaskReplicate(uuid, id, node));
            try {
                jobDir = Files.initDirectory(new File(rootDir, id + File.separator + node + File.separator + "live"));
                log.warn("[task.execReplicate] replicating " + jobDir.getPath());
                File configDir = getConfigDir();
                Files.initDirectory(configDir);
                // create shell wrapper
                replicateSH = new File(configDir, "replicate.sh");
                replicateRun = new File(configDir, "replicate.run");
                replicateDone = new File(configDir, "replicate.done");
                replicatePid = new File(configDir, "replicate.pid");
                if (execute) {
                    require(deleteFiles(replicatePid, replicateDone), "failed to delete replicate config files");
                    String replicateRunScript = generateRunScript(replicateSH.getCanonicalPath(), replicatePid.getCanonicalPath(), replicateDone.getCanonicalPath());
                    Files.write(replicateRun, Bytes.toBytes(replicateRunScript), false);
                    String replicateSHScript = generateReplicateSHScript(replicateAllBackups);
                    Files.write(replicateSH, Bytes.toBytes(replicateSHScript), false);
                }
                replicateStartTime = System.currentTimeMillis();
                // save it
                save();
                sendHostStatus();
                // start watcher
                workItemThread = new Thread(new ReplicateWorkItem(jobDir, replicatePid, replicateRun, replicateDone, this, choreWatcherKey, execute));
                workItemThread.setName("Replicate-WorkItem-" + getName());
                workItemThread.start();
            } catch (Exception ex) {
                sendEndStatus(JobTaskErrorCode.EXIT_SCRIPT_EXEC_ERROR);
                throw ex;
            }
        }

        public void execBackup(String choreWatcherKey, boolean execute) throws Exception {
            if (log.isDebugEnabled()) {
                log.debug("[task.execBackup] " + this.getJobKey());
            }
            require(testTaskIdle(), "task is not idle");
            sendStatusMessage(new StatusTaskBackup(uuid, id, node));
            try {
                log.warn("[task.execBackup] backing up " + jobDir.getPath());
                File configDir = getConfigDir();
                Files.initDirectory(configDir);
                backupSH = new File(configDir, "backup.sh");
                backupRun = new File(configDir, "backup.run");
                backupDone = new File(configDir, "backup.done");
                backupPid = new File(configDir, "backup.pid");
                if (execute) {
                    require(deleteFiles(backupPid, backupDone), "failed to delete backup config files");
                    String backupSHScript = generateBackupSHScript(replicas);
                    Files.write(backupSH, Bytes.toBytes(backupSHScript), false);
                    String backupRunScript = generateRunScript(backupSH.getCanonicalPath(), backupPid.getCanonicalPath(), backupDone.getCanonicalPath());
                    Files.write(backupRun, Bytes.toBytes(backupRunScript), false);
                }
                backupStartTime = System.currentTimeMillis();
                save();
                sendHostStatus();
                workItemThread = new Thread(new BackupWorkItem(jobDir, backupPid, backupRun, backupDone, this, choreWatcherKey, execute));
                workItemThread.setName("Backup-WorkItem-" + getName());
                workItemThread.start();
            } catch (Exception ex) {
                sendEndStatus(JobTaskErrorCode.EXIT_SCRIPT_EXEC_ERROR);
                throw ex;
            }
        }

        private String makeRetryDefinition() {
            StringBuilder sb = new StringBuilder();
            sb.append("retries=" + copyRetryLimit + "\n");
            sb.append("retryDelaySeconds=" + copyRetryDelaySeconds + "\n");
            sb.append("function retry {\n" +
                      "try=0; cmd=\"$@\"\n" +
                      "until [ $try -ge $retries ]; do\n" +
                      "\tif [ \"$try\" -ge \"1\" ]; then echo starting retry $try; sleep $retryDelaySeconds; fi\n" +
                      "\ttry=$((try+1)); eval $cmd; exitCode=$?\n" +
                      "\tif [ \"$exitCode\" == \"0\" ]; then return 0; fi\n" +
                      "done\n" +
                      "echo \"Command failed after $retries retries: $cmd\"; exit $exitCode\n" +
                      "}\n");
            return sb.toString();
        }

        private String generateReplicateSHScript(boolean replicateAllBackups) throws IOException {
            File logDir = new File(jobDir, "log");
            Files.initDirectory(logDir);
            StringBuilder bash = new StringBuilder("#!/bin/bash\n");
            bash.append(makeRetryDefinition());
            bash.append(echoWithDate_cmd + "Deleting environment lock files in preparation for replication\n");
            bash.append("find " + jobDir.getCanonicalPath() + " -name je.lck -print -exec rm {} \\;\n");
            bash.append("find " + jobDir.getCanonicalPath() + " -name je.info.0 -print -exec rm {} \\;\n");
            appendReplicas(bash, failureRecoveryReplicas, true); // Add commands for any the failure-recovery replicas that definitely need full rsyncs
            appendReplicas(bash, replicas, replicateAllBackups); // Add commands for the existing replicas
            bash.append(echoWithDate_cmd + "Finished replicating successfully\n");
            return bash.toString();
        }

        private void appendReplicas(StringBuilder bash, ReplicaTarget[] replicas, boolean replicateAllBackups) throws IOException {
            if (replicas == null) {
                return;
            }
            for (ReplicaTarget replica : replicas) {
                if (replica.getHostUuid() == null || replica.getHostUuid().equals(uuid)) {
                    return;
                }
                List<String> replicateCommands = assembleReplicateCommandAndInformSpawn(replica, replicateAllBackups);
                if (replicateCommands == null || replicateCommands.isEmpty()) {
                    return;
                }
                String action = "replicating to " + replica.getHost() + " uuid=" + replica.getHostUuid();
                appendCommandsWithStartFinishMessages(bash, action, replicateCommands, replicateCommandDelaySeconds);
            }
        }

        private String generateRunScript(String shName, String pidPath, String donePath) throws IOException {
            if (logOut == null || logErr == null) {
                File logRoot = new File(jobDir, "log");
                logOut = new File(logRoot, "log.out");
                logErr = new File(logRoot, "log.err");
            }
            StringBuilder bash = new StringBuilder("#!/bin/bash\n");
            bash.append("(\n");
            bash.append("\t cd " + jobDir.getCanonicalPath() + "\n");
            bash.append("\t (bash " + shName + ") &\n");
            bash.append("\t pid=$!\n");
            bash.append("\t echo ${pid} > " + pidPath + "\n");
            bash.append("\t exit=0\n");
            bash.append("\t wait ${pid} || exit=$?\n");
            bash.append("\t echo ${exit} > " + donePath + "\n");
            bash.append("\t exit ${exit};\n");
            bash.append(") >> " + logOut.getCanonicalPath() + " 2>> " + logErr.getCanonicalPath() + " &");
            return bash.toString();
        }

        private String generateBackupSHScript(ReplicaTarget[] replicas) throws IOException {
            File logDir = new File(jobDir, "log");
            Files.initDirectory(logDir);
            StringBuilder bash = new StringBuilder("#!/bin/bash\n");
            bash.append("cd " + jobDir.getCanonicalPath() + "\n");
            bash.append(makeRetryDefinition());
            List<String> symlinkCommands = new ArrayList<String>();
            List<String> deleteCommands = new ArrayList<String>();
            List<String> localBackupCommands = assembleBackupCommandsForHost(true, null, symlinkCommands, deleteCommands);
            appendCommandsWithStartFinishMessages(bash, "updating local backups", localBackupCommands, backupCommandDelaySeconds);
            if (replicas != null) {
                for (ReplicaTarget replica : replicas) {
                    if (replica.getHostUuid() == null || replica.getHostUuid().equals(uuid)) {
                        continue;
                    }
                    String action = "updating backups on " + replica.getHost() + " uuid=" + replica.getHostUuid();
                    List<String> remoteBackupCommands = assembleBackupCommandsForHost(false, replica, symlinkCommands, deleteCommands);
                    appendCommandsWithStartFinishMessages(bash, action, remoteBackupCommands, backupCommandDelaySeconds);
                }
            }
            appendCommandsWithStartFinishMessages(bash, "updating symlinks", symlinkCommands, backupCommandDelaySeconds);
            appendCommandsWithStartFinishMessages(bash, "deleting old backups", deleteCommands, backupCommandDelaySeconds);
            bash.append(echoWithDate_cmd + "Finished backing up successfully\n");
            return bash.toString();
        }

        private void appendCommandsWithStartFinishMessages(StringBuilder builder, String action, List<String> commands, int delaySeconds) {
            builder.append(echoWithDate_cmd + " Started " + action + " \n");
            for (String cmd : commands) {
                if (delaySeconds > 0) {
                    builder.append("sleep " + delaySeconds + " && \\\n");
                }
                builder.append(cmd + " && \\\n");
            }
            builder.append(echoWithDate_cmd + " Finished " + action + " \n");
        }

        /**
         * Suppose we have received a message to begin running a task / replicating / backing up.
         * If we're already doing one of these, reject the received instruction and re-send an event describing what we're doing.
         *
         * @return true only if the task was really idle.
         */
        private boolean testTaskIdle() {
            if (isRunning()) {
                sendStatusMessage(new StatusTaskBegin(uuid, id, node));
                return false;
            } else if (isReplicating()) {
                sendStatusMessage(new StatusTaskReplicate(uuid, id, node));
                return false;
            } else if (isBackingUp()) {
                sendStatusMessage(new StatusTaskBackup(uuid, id, node));
                return false;
            } else if (workItemThread != null) {
                log.warn("clearing workItem for idle task " + getName());
                workItemThread.interrupt();
                workItemThread = null;
            }
            return true;
        }

        private boolean isProcessRunning(File pidFile) {
            Integer pid = getPID(pidFile);
            return pid != null && activeProcessExistsWithPid(pid, rootDir);
        }

        protected void createDoneFileIfNoProcessRunning(File pidFile, File doneFile) {
            if (doneFile == null || pidFile == null || doneFile.exists()) {
                return;
            }
            boolean success = false;
            try {
                Integer pid = getPID(pidFile);
                if (pid == null || !activeProcessExistsWithPid(pid, rootDir)) {
                    success = doneFile.exists() || doneFile.createNewFile();
                } else {
                    success = true; // Process exists, nothing to do.
                }
            } catch (IOException io) {
                success = false;
                log.warn("[task.state.check] exception when creating done file: " + io, io);
                }
            if (!success) {
                log.warn("[task.state.check] failed to create done file for task " + getName() + " path " + doneFile);
            }
        }

        public String getName() {
            return id + "/" + node;
        }

        public File getJobDir() {
            return jobDir;
        }

        public Integer getPort() {
            try {
                if (port == null && jobPort.exists())// && jobPort.lastModified() >= jobRun.lastModified())
                {
                    port = Integer.parseInt(Bytes.toString(Files.read(jobPort)));
                }
            } catch (Exception ex)  {
                log.warn("", ex);
            }
            return port;
        }

        // TODO hookup to a job clean cmd at some point (for testing mostly)
        public boolean deleteData() {
            return false;
        }

        public boolean isRunning() {
            if (jobDone == null) {
                return false;
            }
            // no checking for process here since this doesn't seem to be broken like the others
            return this.startTime > 0 && !jobDone.exists();
        }

        public boolean isReplicating() {
            if (replicateDone == null) {
                return false;
            }
            return !isRunning() && replicateStartTime > 0 && !replicateDone.exists() && isProcessRunning(replicatePid);
        }

        public boolean isBackingUp() {
            if (backupDone == null) {
                return false;
            }
            return !isRunning() && !isReplicating() && backupStartTime > 0 && !backupDone.exists() && isProcessRunning(backupPid);
        }

        public File[] getActivePidFiles() {
            if (isRunning()) {
                return new File[]{jobPid};
            } else if (isReplicating()) {
                return new File[]{replicatePid};
            } else if (isBackingUp()) {
                return new File[]{backupPid};
            } else {
                return null;
            }
        }

        public boolean stopWait(boolean kill) {
            File[] activePidFiles = getActivePidFiles();
            Integer rsync;
            if (isReplicating() && ((rsync = findActiveRsync(id, node)) != null)) {
                shell("kill -9 " + rsync, rootDir);
            }
            return activePidFiles != null && stopWait(activePidFiles, kill);
        }

        public boolean stopWait(File[] pidFiles, boolean kill) {
            boolean result = true;
            try {
                if (kill) {
                    resetStartTime();
                    log.warn("[stopWait] creating done files if they do not exist");
                    createDoneFileIfNoProcessRunning(jobPid, jobDone);
                    createDoneFileIfNoProcessRunning(replicatePid, replicateDone);
                    createDoneFileIfNoProcessRunning(backupPid, backupDone);
                }
                for (File pidFile : pidFiles) {
                    Integer pid = getPID(pidFile);
                    if (pid == null) {
                        log.warn((kill ? "stop" : "kill") + "Wait failed with null pid for " + getName());
                        result = false;
                    } else {
                        if (pid.equals(minionPid)) {
                            log.warn("[minion.kill] tried to kill my own process. pid: " + pid);
                            result = false;
                        }
                        String cmd = getCmdLine(pid);
                        if (cmd == null) {
                            log.warn("[minion.kill] unable to read cmdline, so it seems unlikely the process is running, ret false");
                            result = false;
                        } else {
                            log.warn("[minion.kill] about to kill pid " + pid + " with cmd line: " + cmd);
                            if (cmd.contains(" minion") || cmd.contains(" mss") || cmd.contains(" mqworker")) {
                                log.warn("It looked like we are trying to kill an Important Process (TM), returning false instead");
                                result = false;
                            }
                        }
                        jobStopped = new File(jobDir, "job.stopped");
                        if (!jobStopped.createNewFile()) {
                            log.warn("Failed to create job.stopped file for stopped job " + getName());
                        }
                        if (kill) {
                            log.warn("[minion.kill] killing pid:" + pid + " hard");
                            result &= Minion.shell("kill -9 " + pid, rootDir) >= 0;
                        } else {
                            log.warn("[minion.kill] killing pid:" + pid + " nice");
                            result &= Minion.shell("kill " + pid, rootDir) >= 0;
                        }
                    }
                }
            } catch (Exception ex)  {
                log.warn("", ex);
            }
            return result;
        }

        private void resetStartTime() {
            if (isRunning()) {
                startTime = 0;
            } else if (isReplicating()) {
                replicateStartTime = 0;
            } else if (isBackingUp()) {
                backupStartTime = 0;
            }
            writeState();
        }

        public File getLiveDir() {
            return new File(taskRoot, "live");
        }

        public File getConfigDir() {
            return new File(taskRoot, "config");
        }

        public String profile() {
            File profile = new File(jobDir, "job.profile");
            if (profile.exists()) {
                try {
                    return Bytes.toString(Files.read(profile));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return "";
        }

        public JSONObject readLogLines(File file, int startOffset, int lines) {
            JSONObject json = new JSONObject();
            String content = "";
            long off = 0;
            long endOffset = 0;
            int linesRead = 0;
            int bytesRead = 0;
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                long len = raf.length();
                //if startoffset is negative, tail the content
                if (startOffset < 0 || startOffset > len) {
                    off = len;
                    while (lines > 0 && --off >= 0) {
                        raf.seek(off);
                        if (off == 0 || raf.read() == '\n') {
                            lines--;
                            linesRead++;
                        }
                    }
                    bytesRead = (int) (len - off);
                    byte buf[] = new byte[bytesRead];
                    raf.read(buf);
                    content = Bytes.toString(buf);
                    endOffset = len;
                } else if (len > 0 && startOffset < len) {
                    off = startOffset;
                    while (lines > 0 && off < len) {
                        raf.seek(off++);
                        if (raf.read() == '\n') {
                            lines--;
                            linesRead++;
                        }
                    }
                    bytesRead = (int) (off - startOffset);
                    byte buf[] = new byte[bytesRead];
                    raf.seek(startOffset);
                    raf.read(buf);
                    content = Bytes.toString(buf);
                    endOffset = off;
                } else if (startOffset == len) {
                    endOffset = len;
                    linesRead = 0;
                    content = "";
                }
                json.put("offset", endOffset);
                json.put("lines", linesRead);
                json.put("lastModified", file.lastModified());
                json.put("out", content);
            } catch (Exception e)  {
                log.warn("", e);
            }
            return json;
        }

        public String tail(File file, int lines) {
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                long len = raf.length();
                if (len <= 0) {
                    return "";
                }
                long off = len;
                while (lines > 0 && --off >= 0) {
                    raf.seek(off);
                    if (off == 0 || raf.read() == '\n') {
                        lines--;
                    }
                }
                byte buf[] = new byte[(int) (len - off)];
                raf.read(buf);
                return Bytes.toString(buf);
            } catch (Exception e)  {
                log.warn("", e);
            }
            return "";
        }

        public String head(File file, int lines) {
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                long len = raf.length();
                if (len <= 0) {
                    return "";
                }
                long off = 0;
                while (lines > 0 && off < len) {
                    raf.seek(off++);
                    if (raf.read() == '\n') {
                        lines--;
                    }
                }
                byte buf[] = new byte[(int) off];
                raf.seek(0);
                raf.read(buf);
                return Bytes.toString(buf);
            } catch (Exception e)  {
                log.warn("", e);
            }
            return "";
        }

        public void setRuntime(long runTime) {
            this.runTime = runTime;
        }

        public void setReplicas(ReplicaTarget[] replicas) {
            this.replicas = replicas;
        }

        public void setFailureRecoveryReplicas(ReplicaTarget[] replicas) {
            this.failureRecoveryReplicas = replicas;
        }

        public ReplicaTarget[] getFailureRecoveryReplicas() {
            return failureRecoveryReplicas;
        }

        public ReplicaTarget[] getReplicas() {
            return replicas;
        }

        public boolean wasStopped() {
            if (jobStopped == null) {
                jobStopped = new File(jobDir, "job.stopped");
            }
            return jobStopped.exists();
        }

        @Override
        public String toString() {
            return "JobTask{" +
                   "id='" + id + '\'' +
                   ", node=" + node +
                   ", jobDir=" + jobDir +
                   '}';
        }
    }

    public static class FileStats {

        public long count;
        public long bytes;

        private void update(File dir) {
            if (dir != null) {
                for (File file : dir.listFiles()) {
                    if (file.isDirectory()) {
                        update(file);
                    } else if (file.isFile()) {
                        count++;
                        bytes += file.length();
                    }
                }
            } else {
                count = 0;
                bytes = 0;
            }
        }
    }

    @Override
    public void handle(String target, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
        try {
            doHandle(target, request, httpServletRequest, httpServletResponse);
        } catch (IOException io) {
            throw io;
        } catch (ServletException se) {
            throw se;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public void doHandle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws Exception {
        response.setBufferSize(65535);
        KVPairs kv = new KVPairs();
        boolean handled = true;
        for (Enumeration<String> e = request.getParameterNames(); e.hasMoreElements();) {
            String k = e.nextElement();
            String v = request.getParameter(k);
            kv.add(k, v);
        }
        if (target.equals("/ping")) {
            response.getWriter().write("ACK");
        } else if (target.startsWith("/metrics")) {
            metricsHandler.handle(target, baseRequest, request, response);
        } else if (target.equals("/job.port")) {
            String job = kv.getValue("id");
            int taskID = kv.getIntValue("node", -1);
            JobKey key = new JobKey(job, taskID);
            JobTask task = tasks.get(key.toString());
            Integer jobPort = null;
            if (task != null) {
                jobPort = task.getPort();
            }
            response.getWriter().write("{port:" + (jobPort != null ? jobPort : 0) + "}");
        } else if (target.equals("/job.profile")) {
            String jobName = kv.getValue("id", "") + "/" + kv.getIntValue("node", 0);
            JobTask job = tasks.get(jobName);
            if (job != null) {
                response.getWriter().write(job.profile());
            } else {
                response.sendError(400, "No Job");
            }
        } else if (target.equals("/job.head")) {
            String jobName = kv.getValue("id", "") + "/" + kv.getIntValue("node", 0);
            int lines = kv.getIntValue("lines", 10);
            boolean out = !kv.getValue("out", "0").equals("0");
            boolean err = !kv.getValue("err", "0").equals("0");
            String html = kv.getValue("html");
            JobTask job = tasks.get(jobName);
            if (job != null) {
                String outB = out ? job.head(job.logOut, lines) : "";
                String errB = err ? job.head(job.logErr, lines) : "";
                if (html != null) {
                    html = html.replace("{{out}}", outB);
                    html = html.replace("{{err}}", errB);
                    response.setContentType("text/html");
                    response.getWriter().write(html);
                } else {
                    response.getWriter().write(new JSONObject().put("out", outB).put("err", errB).toString());
                }
            } else {
                response.sendError(400, "No Job");
            }
        } else if (target.equals("/job.tail")) {
            String jobName = kv.getValue("id", "") + "/" + kv.getIntValue("node", 0);
            int lines = kv.getIntValue("lines", 10);
            boolean out = !kv.getValue("out", "0").equals("0");
            boolean err = !kv.getValue("err", "0").equals("0");
            String html = kv.getValue("html");
            JobTask job = tasks.get(jobName);
            if (job != null) {
                String outB = out ? job.tail(job.logOut, lines) : "";
                String errB = err ? job.tail(job.logErr, lines) : "";
                if (html != null) {
                    html = html.replace("{{out}}", outB);
                    html = html.replace("{{err}}", errB);
                    response.setContentType("text/html");
                    response.getWriter().write(html);
                } else {
                    response.getWriter().write(new JSONObject().put("out", outB).put("err", errB).toString());
                }
            } else {
                response.sendError(400, "No Job");
            }
        } else if (target.equals("/job.log")) {
            String jobName = kv.getValue("id", "") + "/" + kv.getIntValue("node", 0);
            int offset = kv.getIntValue("offset", -1);
            int lines = kv.getIntValue("lines", 10);
            boolean out = kv.getValue("out", "1").equals("1");
            JobTask job = tasks.get(jobName);
            File log = (out ? job.logOut : job.logErr);
            if (job != null) {
                JSONObject logJson = job.readLogLines(log, offset, lines);
                //for JSONP support
                if (kv.hasKey("callback")) {
                    String callback = kv.getValue("callback");
                    response.getWriter().write(callback + "(" + logJson.toString() + ")");
                } else {
                    response.getWriter().write(logJson.toString());
                }
            } else {
                response.sendError(400, "No Job");
            }
        } else if (target.equals("/jobs.import")) {
            int count = updateJobsMeta(new File(kv.getValue("dir", ".")));
            response.getWriter().write("imported " + count + " jobs");
        } else if (target.equals("/xdebug/findnextport")) {
            int port = findNextPort();
            response.getWriter().write("port: " + port);
        } else if (target.equals("/active.tasks")) {
            capacityLock.lock();
            try {
                response.getWriter().write("tasks: " + activeTaskKeys.toString());
            } finally {
                capacityLock.unlock();
            }
        } else if (target.equals("/task.size")) {
            String jobId = kv.getValue("id");
            int taskId = kv.getIntValue("node", -1);
            if (jobId != null && taskId >= 0) {
                String duOutput = new SimpleExec(ducmd + " -s --block-size=1 " + getTaskBaseDir(rootDir.getAbsolutePath(), jobId, taskId)).join().stdoutString();
                response.getWriter().write(
                        duOutput.split("\t")[0]
                );
            }
        } else {
            response.sendError(404);
        }
        ((Request) request).setHandled(handled);
    }

    private class CommandTaskDeleteRunner implements Runnable {

        CoreMessage core;

        public CommandTaskDeleteRunner(CoreMessage core) {
            this.core = core;
        }

        @Override
        public void run() {
            CommandTaskDelete delete = (CommandTaskDelete) core;
            log.warn("[task.delete] " + delete.getJobKey());
            minionStateLock.lock();
            try {
                for (JobTask task : getMatchingJobs(delete)) {
                    stopped.put(delete.getJobUuid(), delete.getRunCount());
                    boolean terminated = task.isRunning() && task.stopWait(true);
                    task.setDeleted(true);
                    tasks.remove(task.getJobKey().toString());
                    log.warn("[task.delete] " + task.getJobKey() + " terminated=" + terminated);
                    writeState();
                }
                File taskDirFile = new File(rootDir + "/" + delete.getJobUuid() + (delete.getNodeID() != null ? "/" + delete.getNodeID() : ""));
                if (taskDirFile.exists() && taskDirFile.isDirectory()) {
                    minionTaskDeleter.submitPathToDelete(taskDirFile.getAbsolutePath());
                }
            } finally {
                minionStateLock.unlock();
            }

        }
    }

    private class CommandTaskStopRunner implements Runnable {

        private CoreMessage core;

        public CommandTaskStopRunner(CoreMessage core) {
            this.core = core;
        }

        @Override
        public void run() {
            CommandTaskStop stop = (CommandTaskStop) core;
            log.warn("[task.stop] request " + stop.getJobKey() + " count @ " + stop.getRunCount());
            removeJobFromQueue(stop.getJobKey(), false);
            if (stop.getJobKey().getNodeNumber() == null) {
                minionStateLock.lock();
                try {
                    stopped.put(stop.getJobUuid(), stop.getRunCount());
                } finally {
                    minionStateLock.unlock();
                }
            }
            List<JobTask> match = getMatchingJobs(stop);
            if (match.size() == 0 && stop.getNodeID() != null && stop.getNodeID() >= 0) {
                log.warn("[task.stop] unmatched stop for " + stop.getJobUuid() + " / " + stop.getNodeID());
                sendStatusMessage(new StatusTaskEnd(uuid, stop.getJobUuid(), stop.getNodeID(), 0, 0, 0));
            }
            for (JobTask task : match) {
                boolean terminated = false;
                if (!task.getConfigDir().exists()) {
                    Files.initDirectory(task.getConfigDir());
                }
                if (task.isRunning() || task.isReplicating() || task.isBackingUp()) {
                    if (!stop.force() && (task.isReplicating() || task.isBackingUp())) {
                        log.warn("[task.stop] " + task.getName() + " wasn't terminated because task was replicating/backing up and the stop wasn't a kill");
                    } else if (!stop.getOnlyIfQueued()) {
                        task.stopWait(stop.force());
                        log.warn("[task.stop] " + task.getName() + " terminated=" + terminated);
                    } else {
                        log.warn("[task.stop] " + task.getName() + " wasn't terminated because task was running and the stop specified only-if-queued");
                    }
                } else if (task.kick != null && task.kick.getRunCount() != stop.getRunCount()) {
                    log.warn("[task.stop] " + task.getName() + " before exec count @ " + task.kick.getRunCount() + " != " + stop.getRunCount());
                    task.sendEndStatus(0);
                } else if (stop.force()) {
                    log.warn("[task.stop] " + task.getName() + " force stop unmatched");
                    task.createDoneFileIfNoProcessRunning(task.jobPid, task.jobDone);
                    task.createDoneFileIfNoProcessRunning(task.replicatePid, task.replicateDone);
                    task.createDoneFileIfNoProcessRunning(task.backupPid, task.backupDone);
                    if (task.jobDone != null && task.jobDone.exists()) {
                        int endStatus = 0;
                        try {
                            // Try to get last end state from done file
                            endStatus = Integer.parseInt(Bytes.toString(Files.read(task.jobDone)).trim());
                        } catch (Exception ex) {
                            // If not, just send 0 so Spawn ends up in a happy state
                        }
                        task.sendEndStatus(endStatus);
                    }
                }
            }
            writeState();
        }

    }

    private void removeJobFromQueue(JobKey key, boolean sendToSpawn) {
        minionStateLock.lock();
        try {
            for (Iterator<CommandTaskKick> iter = jobQueue.iterator(); iter.hasNext();) {
                CommandTaskKick kick = iter.next();
                if (kick.getJobKey().matches(key)) {
                    log.warn("[task.stop] removing from queue " + kick.getJobKey() + " kick=" + kick + " key=" + key);
                    if (sendToSpawn) {
                        try {
                            sendStatusMessage(new StatusTaskEnd(uuid, kick.getJobUuid(), kick.getNodeID(), 0, 0, 0));
                        } catch (Exception ex)  {
                            log.warn("", ex);
                            log.warn("---> send fail " + uuid + " " + key + " " + kick);
                        }
                    }
                    iter.remove();
                }
            }
        } finally {
            minionStateLock.unlock();
        }
    }

    private class CommandTaskRevertRunner implements Runnable {

        CoreMessage core;

        public CommandTaskRevertRunner(CoreMessage core) {
            this.core = core;
        }

        @Override
        public void run() {
            CommandTaskRevert revert = (CommandTaskRevert) core;
            List<JobTask> match = getMatchingJobs(revert);
            log.warn("[task.revert] request " + revert.getJobKey() + " matched " + match.size());
            if (match.size() == 0 && revert.getNodeID() != null && revert.getNodeID() >= 0) {
                log.warn("[task.revert] unmatched for " + revert.getJobUuid() + " / " + revert.getNodeID());
            }
            if (revert.getNodeID() == null || revert.getNodeID() < 0) {
                log.warn("[task.revert] got invalid node id " + revert.getNodeID());
                return;
            }
            for (JobTask task : match) {
                if (task.isRunning() || task.isReplicating() || task.isBackingUp()) {
                    log.warn("[task.revert] " + task.getJobKey() + " skipped. job node active.");
                } else {
                    long time = System.currentTimeMillis();
                    task.setReplicas(revert.getReplicas());
                    if (revert.getSkipMove()) {
                        try {
                            task.execReplicate(null, false, true);
                        } catch (Exception ex) {
                            task.sendEndStatus(JobTaskErrorCode.EXIT_REVERT_FAILURE);
                        }
                    } else {
                        task.revertToBackup(revert.getRevision(), revert.getTime(), revert.getBackupType());
                    }
                    log.warn("[task.revert] " + task.getJobKey() + " completed in " + (System.currentTimeMillis() - time) + "ms.");
                }
            }
            writeState();
        }
    }

    private class CommandTaskReplicateRunner implements Runnable {

        private CoreMessage core;

        public CommandTaskReplicateRunner(CoreMessage core) {
            this.core = core;
        }

        @Override
        public void run() {
            CommandTaskReplicate replicate = (CommandTaskReplicate) core;
            JobTask task = tasks.get(replicate.getJobKey().toString());
            if (task != null) {
                if (task.jobDir == null) {
                    task.jobDir = task.getLiveDir();
                }
                if (!task.jobDir.exists()) {
                    log.warn("[task.replicate] aborted because there is no directory for " + task.getJobKey() + " yet: " + task.jobDir);
                } else if (!task.isRunning() && !task.isReplicating() && !task.isBackingUp()) {
                    log.warn("[task.replicate] starting " + replicate.getJobKey());
                    removeJobFromQueue(replicate.getJobKey(), false);
                    if (!task.isComplete()) {
                        // Attempt to revert to the latest complete backup, if one can be found
                        String latestCompleteBackup = task.getBackupByRevision(0, "gold");
                        if (latestCompleteBackup != null) {
                            task.promoteBackupToLive(new File(task.getJobDir(), latestCompleteBackup), task.getLiveDir());
                        }
                    }
                    try {
                        task.setReplicas(replicate.getReplicas());
                        task.execReplicate(replicate.getChoreWatcherKey(), true, true);
                    } catch (Exception e) {
                        log.warn("[task.replicate] received exception after replicate request for " + task.getJobKey() + ": " + e, e);
                    }
                } else {
                    log.warn("[task.replicate] skip running " + replicate.getJobKey());
                }
            }
        }
    }

    private class CommandCreateNewTask implements Runnable {

        private CoreMessage core;

        public CommandCreateNewTask(CoreMessage core) {
            this.core = core;
        }

        @Override
        public void run() {
            CommandTaskNew newTask = (CommandTaskNew) core;
            JobTask task = tasks.get(newTask.getJobKey().toString());
            if (task == null) {
                log.warn("[task.new] creating " + newTask.getJobKey());
                try {
                    createNewTask(newTask.getJobUuid(), newTask.getNodeID());
                } catch (ExecException e) {
                    log.warn("Error restoring task state: " + e, e);
                    }
            } else {
                // Make sure the id/node # were set correctly
                task.id = newTask.getJobUuid();
                task.node = newTask.getNodeID();
                log.warn("[task.new] skip existing " + newTask.getJobKey());
            }
        }
    }

    private class CommandTaskUpdateReplicasRunner implements Runnable {

        private CoreMessage core;

        public CommandTaskUpdateReplicasRunner(CoreMessage core) {
            this.core = core;
        }

        @Override
        public void run() {
            CommandTaskUpdateReplicas updateReplicas = (CommandTaskUpdateReplicas) core;
            JobTask task = tasks.get(updateReplicas.getJobKey().toString());
            if (task != null) {
                synchronized (task) {
                    List<ReplicaTarget> newReplicas = new ArrayList<>();
                    if (task.replicas != null) {
                        for (ReplicaTarget replica : task.replicas) {
                            if (!updateReplicas.getFailedHosts().contains(replica.getHostUuid())) {
                                newReplicas.add(replica);
                            }
                        }
                    }
                    task.setReplicas(newReplicas.toArray(new ReplicaTarget[newReplicas.size()]));
                    List<ReplicaTarget> failureRecoveryReplicas = updateReplicas.getNewReplicaHosts();
                    task.setFailureRecoveryReplicas(failureRecoveryReplicas.toArray(new ReplicaTarget[failureRecoveryReplicas.size()]));
                }
            }
        }
    }

    private JobTask createNewTask(String jobID, int node) throws ExecException {
        Minion.JobTask task = new JobTask();
        task.id = jobID;
        task.node = node;
        task.taskRoot = new File(rootDir, task.id + "/" + task.node);
        log.warn("[task.new] restore " + task.id + "/" + task.node + " root=" + task.taskRoot);
        tasks.put(task.getJobKey().toString(), task);
        task.initializeFileVariables();
        writeState();
        return task;
    }

    /**
     * Delete a series of files.
     *
     * @param files Files to delete
     * @return False only if some file existed and could not be deleted
     */
    private boolean deleteFiles(File... files) {
        for (File file : files) {
            if (file != null && file.exists()) {
                if (shell(rmcmd + " -rf " + file.getAbsolutePath(), rootDir) != 0) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean activeProcessExistsWithPid(Integer pid, File directory) {
        return shell("ps " + pid, directory) == 0;
    }

    private static Integer findActiveRsync(String id, int node) {
        return findActiveProcessWithTokens(new String[]{id + "/" + node + "/", rsyncCommand}, new String[]{"gold"});
    }

    private static Integer findActiveProcessWithTokens(String[] requireTokens, String[] omitTokens) {
        StringBuilder command = new StringBuilder("ps ax | grep -v grep");
        for (String requireToken : requireTokens) {
            command.append("| grep " + requireToken);
        }
        for (String excludeToken : omitTokens) {
            command.append("| grep -v " + excludeToken);
        }
        command.append("| cut -c -5");
        try {
            SimpleExec exec = new SimpleExec(new String[]{"/bin/sh", "-c", command.toString()}).join();
            if (exec.exitCode() == 0) {
                return new Scanner(exec.stdoutString()).nextInt();
            } else {
                return null;
            }
        } catch (Exception e) {
            // No PID found
            return null;
        }

    }

    /**
     * This lightweight thread periodically updates various minion metrics
     */
    private class HostMetricUpdater extends Thread {

        HostMetricUpdater() {
            setDaemon(true);
            start();
        }

        public void run() {
            while (!shutdown.get()) {
                try {
                    Thread.sleep(hostMetricUpdaterInterval);
                    activeTaskHistogram.update(activeTaskKeys.size());
                    diskFree.set(rootDir.getFreeSpace());
                } catch (Exception ex) {
                    if (!(ex instanceof InterruptedException)) {
                        log.warn("Exception during host metric update: " + ex, ex);
                        }
                }
            }
        }
    }

    public boolean getShutdown() {
        return shutdown.get();
    }

    public static String getDefaultMinionType() {
        return defaultMinionType;
    }

}

