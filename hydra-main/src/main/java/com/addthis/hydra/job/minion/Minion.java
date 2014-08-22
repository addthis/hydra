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
package com.addthis.hydra.job.minion;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import java.lang.management.ManagementFactory;

import java.net.InetAddress;
import java.net.ServerSocket;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
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
import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Numbers;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.SimpleExec;

import com.addthis.bark.ZkGroupMembership;
import com.addthis.bark.ZkUtil;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.JobTaskErrorCode;
import com.addthis.hydra.job.mq.CommandTaskKick;
import com.addthis.hydra.job.mq.CoreMessage;
import com.addthis.hydra.job.mq.HostCapacity;
import com.addthis.hydra.job.mq.HostMessage;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.mq.JobKey;
import com.addthis.hydra.job.mq.JobMessage;
import com.addthis.hydra.job.mq.StatusTaskCantBegin;
import com.addthis.hydra.job.mq.StatusTaskEnd;
import com.addthis.hydra.mq.MeshMessageConsumer;
import com.addthis.hydra.mq.MeshMessageProducer;
import com.addthis.hydra.mq.MessageConsumer;
import com.addthis.hydra.mq.MessageListener;
import com.addthis.hydra.mq.MessageProducer;
import com.addthis.hydra.mq.RabbitMQUtil;
import com.addthis.hydra.mq.RabbitMessageConsumer;
import com.addthis.hydra.mq.RabbitMessageProducer;
import com.addthis.hydra.mq.RabbitQueueingConsumer;
import com.addthis.hydra.mq.ZKMessageProducer;
import com.addthis.hydra.util.MetricsServletMaker;
import com.addthis.hydra.util.MinionWriteableDiskCheck;
import com.addthis.maljson.JSONObject;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.MeshyClientConnector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.KeeperException;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO implement APIs for extended probing, sanity, clearing of job state
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
public class Minion extends AbstractHandler implements MessageListener, Codable {
    private static final Logger log = LoggerFactory.getLogger(Minion.class);

    static boolean meshQueue = Parameter.boolValue("queue.mesh", false);
    private static final String meshHost = Parameter.value("mesh.host", "localhost");
    private static final int meshPort = Parameter.intValue("mesh.port", 5000);
    private static final int meshRetryTimeout = Parameter.intValue("mesh.retry.timeout", 5000);
    private static int webPort = Parameter.intValue("minion.web.port", 5051);
    private static int minJobPort = Parameter.intValue("minion.job.baseport", 0);
    private static int maxJobPort = Parameter.intValue("minion.job.maxport", 0);
    static ReentrantLock capacityLock = new ReentrantLock();
    private static String dataDir = System.getProperty("minion.data.dir", "minion");
    private static String group = System.getProperty("minion.group", "none");
    private static String localHost = System.getProperty("minion.localhost");
    static boolean linkBackup = !System.getProperty("minion.backup.hardlink", "0").equals("0");
    static final DateTimeFormatter timeFormat = DateTimeFormat.forPattern("yyMMdd-HHmmss");
    private static final String batchBrokerHost = Parameter.value("batch.brokerHost", "localhost");
    private static final String batchBrokerPort = Parameter.value("batch.brokerPort", "5672");
    private static final int mqReconnectDelay = Parameter.intValue("mq.reconnect.delay", 10000);
    private static final int mqReconnectTries = Parameter.intValue("mq.reconnect.tries", 10);
    private static final int sendStatusRetries = Parameter.intValue("send.status.retries", 5);
    private static final int sendStatusRetryDelay = Parameter.intValue("send.status.delay", 5000);
    static final long hostMetricUpdaterInterval = Parameter.longValue("minion.host.metric.interval", 30 * 1000);
    static final String remoteConnectMethod = Parameter.value("minion.remote.connect.method", "ssh -o StrictHostKeyChecking=no -o TCPKeepAlive=yes -o ServerAliveInterval=30");
    static final String rsyncCommand = Parameter.value("minion.rsync.command", "rsync");
    private static final int maxActiveTasks = Parameter.intValue("minion.max.active.tasks", 3);
    static final int copyRetryLimit = Parameter.intValue("minion.copy.retry.limit", 3);
    static final int copyRetryDelaySeconds = Parameter.intValue("minion.copy.retry.delay", 10);
    /* If the following var is positive, it is passed as the bwlimit arg to rsync. If <= 0, it is ignored. */
    static final int copyBandwidthLimit = Parameter.intValue("minion.copy.bwlimit", -1);
    static ReentrantLock revertLock = new ReentrantLock();

    static String cpcmd = "cp";
    static String lncmd = "ln";
    static String lscmd = "ls";
    static String rmcmd = "rm";
    static String mvcmd = "mv";
    static String ducmd = "du";
    static String echoWithDate_cmd = "echo `date '+%y/%m/%d %H:%M:%S'` ";
    static boolean useMacFriendlyPSCommands = false;

    public static final String MINION_ZK_PATH = "/minion/";
    private static final String defaultMinionType = Parameter.value("minion.type", "default");

    // detect fl-cow in sys env and apple for copy command
    static {
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

    public static void main(String[] args) throws Exception {
        new Minion(new File(args.length > 0 ? args[0] : dataDir), args.length > 2 ? Integer.parseInt(args[1]) : webPort);
    }

    @FieldConfig(codable = true, required = true)
    String uuid;
    @FieldConfig(codable = true)
    MinionTaskDeleter minionTaskDeleter;
    @FieldConfig(codable = true)
    ConcurrentHashMap<String, Integer> stopped = new ConcurrentHashMap<>();
    @FieldConfig(codable = true)
    final ArrayList<CommandTaskKick> jobQueue = new ArrayList<>(10);
    @FieldConfig(codable = true)
    String minionTypes;

    final Set<String> activeTaskKeys;
    final AtomicBoolean shutdown = new AtomicBoolean(false);
    final ExecutorService messageTaskExecutorService = MoreExecutors.getExitingExecutorService(
            new ThreadPoolExecutor(4, 4, 100L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()));
    // This next executor service only serves promote/demote requests, so that these will be performed quickly and not
    // wait on a lengthy revert / delete / etc.
    final ExecutorService promoteDemoteTaskExecutorService = MoreExecutors.getExitingExecutorService(
            new ThreadPoolExecutor(4, 4, 100L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()));
    final ExecutorService connectionExecutorService = MoreExecutors
            .getExitingExecutorService(new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ThreadFactoryBuilder().setNameFormat("rabbitMQConnectionService-%d").build()));
    Lock minionStateLock = new ReentrantLock();
    // Historical metrics
    Timer fileStatsTimer;
    Counter sendStatusFailCount;
    Counter sendStatusFailAfterRetriesCount;
    final int replicateCommandDelaySeconds = Parameter.intValue("replicate.cmd.delay.seconds", 0);
    final int backupCommandDelaySeconds = Parameter.intValue("backup.cmd.delay.seconds", 0);
    private MeshyClientConnector mesh;

    final File rootDir;
    final File stateFile;
    final File liveEverywhereMarkerFile;
    final String myHost;
    long startTime;
    int nextPort;
    String user;
    String path;
    TaskRunner runner;
    final ConcurrentHashMap<String, JobTask> tasks = new ConcurrentHashMap<>();
    final Object jmsxmitlock = new Object();
    final AtomicLong diskTotal = new AtomicLong(0);
    final AtomicLong diskFree = new AtomicLong(0);
    final Server jetty;
    final ServletHandler metricsHandler;
    final boolean readOnly;
    boolean diskReadOnly;
    MinionWriteableDiskCheck diskHealthCheck;
    int minionPid = -1;

    RabbitQueueingConsumer batchJobConsumer;
    BlockingArrayQueue<HostMessage> queuedHostMessages;
    private MessageConsumer batchControlConsumer;
    private MessageProducer queryControlProducer;
    private MessageProducer zkBatchControlProducer;
    private MessageProducer batchControlProducer;
    Channel channel;
    private CuratorFramework zkClient;
    private ZkGroupMembership minionGroupMembership;

    Histogram activeTaskHistogram;

    @VisibleForTesting
    public Minion(CuratorFramework zkClient) {
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
        this.zkClient = zkClient;
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
                    if (zkClient != null && zkClient.getState() == CuratorFrameworkState.STARTED) {
                        minionGroupMembership.removeFromGroup("/minion/up", getUUID());
                        zkClient.close();
                    }
                } catch (Exception e) {
                    log.error("Error shutting down", e);
                }
            }
        });
        activeTaskHistogram = Metrics.newHistogram(Minion.class, "activeTasks");
        new HostMetricUpdater(this);
        try {
            joinGroup();
            connectToMQ();
            updateJobsMeta(rootDir);
            if (liveEverywhereMarkerFile.createNewFile()) {
                log.warn("cutover to live-everywhere tasks");
            }
            writeState();
            runner = new TaskRunner(this);
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
            log.warn("Exception during startup", ex);
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
            log.warn("[init] jetty to > {}ms to start.  on port {}", wait, getJettyPort());
        }
    }

    private void connectToMQ() throws Exception {
        zkBatchControlProducer = new ZKMessageProducer(getZkClient());
        if (meshQueue) {
            log.info("Queueing via Mesh");
            final AtomicBoolean up = new AtomicBoolean(false);
            mesh = new MeshyClientConnector(meshHost, meshPort, 1000, meshRetryTimeout) {
                @Override
                public void linkUp(MeshyClient client) {
                    log.info("connected to mesh on {}", client.toString());
                    up.set(true);
                    synchronized (this) {
                        this.notify();
                    }
                }

                @Override
                public void linkDown(MeshyClient client) {
                    log.info("disconnected from mesh on {}", client.toString());
                }
            };
            while (!up.get()) {
                synchronized (mesh) {
                    mesh.wait(1000);
                }
            }
            batchControlProducer = new MeshMessageProducer(mesh.getClient(), "CSBatchControl");
            queryControlProducer = new MeshMessageProducer(mesh.getClient(), "CSBatchQuery");
            queuedHostMessages = new BlockingArrayQueue<>();
            MeshMessageConsumer jobConsumer = new MeshMessageConsumer(mesh.getClient(), "CSBatchJob", uuid);
            jobConsumer.addRoutingKey(HostMessage.ALL_HOSTS);
            jobConsumer.addMessageListener(new MessageListener() {
                @Override
                public void onMessage(Serializable message) {
                    try {
                        queuedHostMessages.put((HostMessage) message);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            });
            batchControlConsumer = new MeshMessageConsumer(mesh.getClient(), "CSBatchControl", uuid).addRoutingKey(HostMessage.ALL_HOSTS);
            batchControlConsumer.addMessageListener(this);
        } else {
            connectToRabbitMQ();
        }
    }

    private synchronized boolean connectToRabbitMQ() {
        String[] routingKeys = new String[]{uuid, HostMessage.ALL_HOSTS};
        batchControlProducer = new RabbitMessageProducer("CSBatchControl", batchBrokerHost, Integer.valueOf(batchBrokerPort));
        queryControlProducer = new RabbitMessageProducer("CSBatchQuery", batchBrokerHost, Integer.valueOf(batchBrokerPort));
        try {
            Connection connection = RabbitMQUtil.createConnection(batchBrokerHost, Integer.valueOf(batchBrokerPort));
            channel = connection.createChannel();
            channel.exchangeDeclare("CSBatchJob", "direct");
            AMQP.Queue.DeclareOk result = channel.queueDeclare(uuid + ".batchJob", true, false, false, null);
            String queueName = result.getQueue();
            channel.queueBind(queueName, "CSBatchJob", uuid);
            channel.queueBind(queueName, "CSBatchJob", HostMessage.ALL_HOSTS);
            batchJobConsumer = new RabbitQueueingConsumer(channel);
            channel.basicConsume(queueName, false, batchJobConsumer);
            batchControlConsumer = new RabbitMessageConsumer(channel, "CSBatchControl", uuid + ".batchControl", this, routingKeys);
            return true;
        } catch (IOException e) {
            log.error("Error connecting to rabbitmq {}:{}", batchBrokerHost, batchBrokerPort, e);
            return false;
        }
    }

    /**
     * attempt to reconnect up to n times then shut down
     */
    void shutdown() {
        System.exit(1);
    }

    private void disconnectFromMQ() {
        try {
            if (batchControlConsumer != null) {
                batchControlConsumer.close();
            }
        } catch (AlreadyClosedException ace) {
            // do nothing
        } catch (Exception ex) {
            log.warn("", ex);
        }
        try {
            if (queryControlProducer != null) {
                queryControlProducer.close();
            }
        } catch (Exception ex) {
            log.warn("", ex);
        }
        try {
            if (batchControlProducer != null) {
                batchControlProducer.close();
            }
        } catch (AlreadyClosedException ace) {
            // do nothing
        } catch (Exception ex) {
            log.warn("", ex);
        }
        try {
            if (zkBatchControlProducer != null) {
                zkBatchControlProducer.close();
            }
        } catch (Exception ex) {
            log.warn("", ex);
        }
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (AlreadyClosedException ace) {
            // do nothing
        } catch (Exception ex) {
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
        } catch (Exception ex) {
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

    void kickNextJob() throws Exception {
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

    List<JobTask> getMatchingJobs(JobMessage msg) {
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
        } catch (Exception ex) {
            log.warn("", ex);
        }
    }

    private void handleMessage(Serializable message) throws Exception {

        if (message instanceof CoreMessage) {
            CoreMessage core;
            try {
                core = (CoreMessage) message;
            } catch (Exception ex) {
                log.warn("", ex);
                return;
            }
            switch (core.getMessageType()) {
                case STATUS_HOST_INFO:
                    log.debug("[host.status] request for {}", uuid);
                    sendHostStatus();
                    break;
                case CMD_TASK_STOP:
                    messageTaskExecutorService.execute(new CommandTaskStopRunner(this, core));
                    break;
                case CMD_TASK_REVERT:
                    messageTaskExecutorService.execute(new CommandTaskRevertRunner(this, core));
                    break;
                case CMD_TASK_DELETE:
                    messageTaskExecutorService.execute(new CommandTaskDeleteRunner(this, core));
                    break;
                case CMD_TASK_REPLICATE:
                    messageTaskExecutorService.execute(new CommandTaskReplicateRunner(this, core));
                    break;
                case CMD_TASK_PROMOTE_REPLICA:
                    // Legacy; ignore
                    break;
                case CMD_TASK_NEW:
                    messageTaskExecutorService.execute(new CommandCreateNewTask(this, core));
                    break;
                case CMD_TASK_DEMOTE_REPLICA:
                    // Legacy; ignore
                    break;
                case CMD_TASK_UPDATE_REPLICAS:
                    promoteDemoteTaskExecutorService.execute(new CommandTaskUpdateReplicasRunner(this, core));
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
            task = new JobTask(this);
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

    void writeState() {
        minionStateLock.lock();
        try {
            Files.write(stateFile, Bytes.toBytes(CodecJSON.encodeString(this)), false);
        } catch (IOException io) {
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
        LinkedList<JobKey> queued = new LinkedList<>();
        minionStateLock.lock();
        try {
            for (CommandTaskKick kick : jobQueue) {
                queued.add(kick.getJobKey());
            }
        } finally {
            minionStateLock.unlock();
        }
        status.setQueued(queued.toArray(new JobKey[queued.size()]));
        status.setMeanActiveTasks(activeTaskHistogram.mean() / (maxActiveTasks > 0 ? maxActiveTasks : 1));
        status.setMaxTaskSlots(maxActiveTasks);
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
    private CuratorFramework getZkClient() {
        if (zkClient == null) {
            zkClient = ZkUtil.makeStandardClient();
        }
        zkClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                if (newState == ConnectionState.RECONNECTED) {
                    joinGroup();
                }
            }
        });
        return zkClient;
    }

    @VisibleForTesting
    protected void closeZkClient() {
        if (zkClient != null) {
            zkClient.close();
        }
    }

    protected void joinGroup() {
        minionGroupMembership = new ZkGroupMembership(getZkClient(), true);
        String upPath = MINION_ZK_PATH + "up";
        try {
            if (zkClient.checkExists().forPath(upPath) == null) {
                zkClient.create().creatingParentsIfNeeded().forPath(upPath, null);
            }
        } catch (KeeperException.NodeExistsException e) {
            // someone beat us to it
        } catch (Exception e) {
            log.error("Exception joining group", e);
        }
        log.info("joining group: " + upPath);
        minionGroupMembership.addToGroup(upPath, getUUID(), shutdown);
    }


    void sendControlMessage(HostMessage msg) {
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

    void sendStatusMessage(HostMessage msg) {
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
        if (!shutdown.getAndSet(true)) {
            writeState();
            log.info("[minion] stopping and sending updated stats to spawn");
            sendHostStatus();
            if (runner != null) {
                runner.stopTaskRunner();
            }
            try {
                Thread.sleep(1000);
            } catch (Exception ex) {
                log.warn("", ex);
            }
            disconnectFromMQ();
        }
    }

    @Override
    public void doStart() {
    }

    synchronized int findNextPort() {
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

    String getTaskBaseDir(String baseDir, String id, int node) {
        return new StringBuilder().append(baseDir).append("/").append(id).append("/").append(node).toString();
    }

    public Integer getPID(File pidFile) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("[getpid] " + pidFile + " --> " + (pidFile != null ? pidFile.exists() : "<null>"));
            }
            return (pidFile != null && pidFile.exists()) ? Integer.parseInt(Bytes.toString(Files.read(pidFile)).trim()) : null;
        } catch (Exception ex) {
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
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Headers", "accept, username");
        response.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT");
        KVPairs kv = new KVPairs();
        boolean handled = true;
        for (Enumeration<String> e = request.getParameterNames(); e.hasMoreElements(); ) {
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
            if (job != null) {
                File log = (out ? job.logOut : job.logErr);
                response.getWriter().write(job.readLogLines(log, offset, lines).toString());
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

    void removeJobFromQueue(JobKey key, boolean sendToSpawn) {
        minionStateLock.lock();
        try {
            for (Iterator<CommandTaskKick> iter = jobQueue.iterator(); iter.hasNext(); ) {
                CommandTaskKick kick = iter.next();
                if (kick.getJobKey().matches(key)) {
                    log.warn("[task.stop] removing from queue " + kick.getJobKey() + " kick=" + kick + " key=" + key);
                    if (sendToSpawn) {
                        try {
                            sendStatusMessage(new StatusTaskEnd(uuid, kick.getJobUuid(), kick.getNodeID(), 0, 0, 0));
                        } catch (Exception ex) {
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

    JobTask createNewTask(String jobID, int node) throws ExecException {
        JobTask task = new JobTask(this);
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
    boolean deleteFiles(File... files) {
        for (File file : files) {
            if (file != null && file.exists()) {
                if (shell(rmcmd + " -rf " + file.getAbsolutePath(), rootDir) != 0) {
                    return false;
                }
            }
        }
        return true;
    }

    static boolean activeProcessExistsWithPid(Integer pid, File directory) {
        return shell("ps " + pid, directory) == 0;
    }

    static Integer findActiveRsync(String id, int node) {
        return findActiveProcessWithTokens(new String[]{id + "/" + node + "/", rsyncCommand}, new String[]{"server"});
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

    public boolean getShutdown() {
        return shutdown.get();
    }

    public static String getDefaultMinionType() {
        return defaultMinionType;
    }

}

