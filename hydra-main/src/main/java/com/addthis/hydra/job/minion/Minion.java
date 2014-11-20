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

import com.addthis.bark.ZkGroupMembership;
import com.addthis.bark.ZkUtil;
import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Numbers;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.SimpleExec;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.config.Configs;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.common.util.CloseTask;
import com.addthis.hydra.discovery.MesosServiceDiscoveryUtility;
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
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.MeshyClientConnector;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
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
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TODO implement APIs for extended probing, sanity, clearing of job state
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
public class Minion implements MessageListener, Codable, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Minion.class);

    private static final String meshHost = Parameter.value("mesh.host", "localhost");
    private static final int meshPort = Parameter.intValue("mesh.port", 5000);
    private static final int meshRetryTimeout = Parameter.intValue("mesh.retry.timeout", 5000);
    private static final int webPort = Parameter.intValue("minion.web.port", 5051);
    private static final int minJobPort = Parameter.intValue("minion.job.baseport", 0);
    private static final int maxJobPort = Parameter.intValue("minion.job.maxport", 0);
    private static final String group = System.getProperty("minion.group", "none");
    private static final String localHost = System.getProperty("minion.localhost");
    private String batchBrokerHost = Parameter.value("batch.brokerHost", "localhost");
    private int batchBrokerPort = Parameter.intValue("batch.brokerPort", 5672);
    private static final int sendStatusRetries = Parameter.intValue("send.status.retries", 5);
    private static final int sendStatusRetryDelay = Parameter.intValue("send.status.delay", 5000);
    static final long hostMetricUpdaterInterval = Parameter.longValue("minion.host.metric.interval", 30 * 1000);
    static final String remoteConnectMethod = Parameter.value("minion.remote.connect.method",
                                                              "ssh -o StrictHostKeyChecking=no -o TCPKeepAlive=yes -o ServerAliveInterval=30");
    static final String rsyncCommand = Parameter.value("minion.rsync.command", "rsync");
    private static final int maxActiveTasks = Parameter.intValue("minion.max.active.tasks", 3);
    static final int copyRetryLimit = Parameter.intValue("minion.copy.retry.limit", 3);
    static final int copyRetryDelaySeconds = Parameter.intValue("minion.copy.retry.delay", 10);
    /* If the following var is positive, it is passed as the bwlimit arg to rsync. If <= 0, it is ignored. */
    static final int copyBandwidthLimit = Parameter.intValue("minion.copy.bwlimit", -1);
    static final ReentrantLock revertLock = new ReentrantLock();
    static final ReentrantLock capacityLock = new ReentrantLock();

    static final DateTimeFormatter timeFormat = DateTimeFormat.forPattern("yyMMdd-HHmmss");
    static final String echoWithDate_cmd = "echo `date '+%y/%m/%d %H:%M:%S'` ";

    public static final String MINION_ZK_PATH = "/minion/";
    public static final String defaultMinionType = Parameter.value("minion.type", "default");
    private static final boolean useMesos = Parameter.boolValue("mesos.useMesos", false);
    private static final int minionWebPortIndex = Parameter.intValue("mesos.minionWebPortIndex", 2);
    private static final String minionAppId = Parameter.value("mesos.minionAppId", "hydraworker");

    public static void main(String[] args) throws Exception {
        Minion minion = Configs.newDefault(Minion.class);
        Runtime.getRuntime().addShutdownHook(new Thread(new CloseTask(minion), "Minion Shutdown Hook"));
    }

    @FieldConfig String uuid;
    @FieldConfig MinionTaskDeleter minionTaskDeleter;
    @FieldConfig ConcurrentMap<String, Integer> stopped = new ConcurrentHashMap<>();
    @FieldConfig List<CommandTaskKick> jobQueue = new ArrayList<>(10);
    @FieldConfig String minionTypes;

    final Set<String> activeTaskKeys;
    final AtomicBoolean shutdown = new AtomicBoolean(false);
    final ExecutorService messageTaskExecutorService = MoreExecutors.getExitingExecutorService(
            new ThreadPoolExecutor(4, 4, 100L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()));
    // This next executor service only serves promote/demote requests, so that these will be performed quickly and not
    // wait on a lengthy revert / delete / etc.
    final ExecutorService promoteDemoteTaskExecutorService = MoreExecutors.getExitingExecutorService(
            new ThreadPoolExecutor(4, 4, 100L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()));
    final Lock minionStateLock = new ReentrantLock();
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
    final ConcurrentMap<String, JobTask> tasks = new ConcurrentHashMap<>();
    final Object jmsxmitlock = new Object();
    final AtomicLong diskTotal = new AtomicLong(0);
    final AtomicLong diskFree = new AtomicLong(0);
    final Server jetty;
    final ServletHandler metricsHandler;
    final MinionHandler minionHandler = new MinionHandler(this);
    boolean diskReadOnly;
    MinionWriteableDiskCheck diskHealthCheck;
    int minionPid = -1;
    int mesosWebPort = -1;

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
        this.zkClient = zkClient;
        uuid = UUID.randomUUID().toString();

        // null placeholder for now
        rootDir = null;
        nextPort = 0;
        startTime = 0;
        stateFile = null;
        liveEverywhereMarkerFile = null;
        myHost = null;
        user = null;
        path = null;
        jetty = null;
        metricsHandler = null;
        diskReadOnly = false;
        minionPid = -1;
        activeTaskKeys = new HashSet<>();
    }

    @JsonCreator
    private Minion(@JsonProperty("dataDir") File rootDir,
                   @Nullable @JsonProperty("queueType") String queueType) throws Exception {
        this.rootDir = rootDir;
        nextPort = minJobPort;
        startTime = System.currentTimeMillis();
        stateFile = new File(Files.initDirectory(rootDir), "minion.state");
        liveEverywhereMarkerFile = new File(rootDir, "liveeverywhere.marker");
        if (localHost != null) {
            myHost = localHost;
        } else {
            myHost = InetAddress.getLocalHost().getHostAddress();
        }
        user = new SimpleExec("whoami").join().stdoutString().trim();
        path = rootDir.getAbsolutePath();
        diskTotal.set(rootDir.getTotalSpace());
        diskFree.set(rootDir.getFreeSpace());
        diskReadOnly = false;
        minionTaskDeleter = new MinionTaskDeleter();
        if (stateFile.exists()) {
            CodecJSON.decodeString(this, Bytes.toString(Files.read(stateFile)));
        } else {
            uuid = UUID.randomUUID().toString();
        }
        File minionTypesFile = new File(rootDir, "minion.types");
        minionTypes = minionTypesFile.exists() ? new String(Files.read(minionTypesFile)).replaceAll("\n", "") : defaultMinionType;
        activeTaskKeys = new HashSet<>();
        jetty = new Server(webPort);
        jetty.setHandler(minionHandler);
        jetty.start();
        waitForJetty();
        if (useMesos) {
            Optional<Map<String,Integer>> optionalHostMap = MesosServiceDiscoveryUtility.getTaskHosts(minionAppId, minionWebPortIndex);
            if (optionalHostMap.isPresent()) {
                mesosWebPort = optionalHostMap.get().get(localHost);
            } else {
                throw new RuntimeException("Could not find host map for mesos app: " + minionAppId);
            }
        }
        sendStatusFailCount = Metrics.newCounter(Minion.class, "sendStatusFail-" + getJettyPort() + "-JMXONLY");
        sendStatusFailAfterRetriesCount = Metrics.newCounter(Minion.class,
                                                             "sendStatusFailAfterRetries-" + getJettyPort() +
                                                             "-JMXONLY");
        fileStatsTimer = Metrics.newTimer(Minion.class, "JobTask-byte-size-timer");
        metricsHandler = MetricsServletMaker.makeHandler();
        activeTaskHistogram = Metrics.newHistogram(Minion.class, "activeTasks");
        new HostMetricUpdater(this);
        try {
            joinGroup();
            connectToMQ(queueType);
            updateJobsMeta(rootDir);
            if (liveEverywhereMarkerFile.createNewFile()) {
                log.warn("cutover to live-everywhere tasks");
            }
            writeState();
            if (queueType != null) {
                runner = new TaskRunner(this, "mesh".equals(queueType));
                runner.start();
            }
            diskHealthCheck = new MinionWriteableDiskCheck(this);
            diskHealthCheck.startHealthCheckThread();
            sendHostStatus();
            log.info("[init] up on {}:{} as {} in {}", myHost, getJettyPort(), user, path);
            String processName = ManagementFactory.getRuntimeMXBean().getName();
            minionPid = Integer.valueOf(processName.substring(0, processName.indexOf("@")));
            log.info("[minion.start] pid for minion process is: {}", minionPid);
            minionTaskDeleter.startDeletionThread();
        } catch (Exception ex) {
            log.warn("Exception during startup", ex);
        }
    }

    public File getRootDir() {
        return rootDir;
    }

    public void setDiskReadOnly(boolean disk_read_only) {
        diskReadOnly = disk_read_only;
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

    private void connectToMQ(@Nullable String queueType) throws Exception {
        zkBatchControlProducer = new ZKMessageProducer(getZkClient());
        if ("mesh".equals(queueType)) {
            log.info("[init] connecting to mesh message queue");
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
            batchControlConsumer.addMessageListener(Minion.this);
        } else if ("rabbit".equals(queueType)) {
            log.info("[init] connecting to rabbit message queue");
            connectToRabbitMQ();
        } else if (queueType == null) {
            log.info("[init] skipping message queue");
        } else {
            throw new IllegalArgumentException("queueType (" + queueType +
                                               ") must be either a valid message queue type or null");
        }
    }

    private synchronized boolean connectToRabbitMQ() {
        String[] routingKeys = {uuid, HostMessage.ALL_HOSTS};
        if (useMesos) {
            batchBrokerHost = MesosServiceDiscoveryUtility.getMesosProxy();
            batchBrokerPort = MesosServiceDiscoveryUtility.getAssignedPort(Parameter.value("mesos.rabbitAppId", "rabbitmq"), Parameter.intValue("mesos.rabbitPortIndex", 0));
        }
        batchControlProducer = new RabbitMessageProducer("CSBatchControl", batchBrokerHost, batchBrokerPort);
        queryControlProducer = new RabbitMessageProducer("CSBatchQuery", batchBrokerHost, batchBrokerPort);
        try {
            Connection connection = RabbitMQUtil.createConnection(batchBrokerHost, batchBrokerPort);
            channel = connection.createChannel();
            channel.exchangeDeclare("CSBatchJob", "direct");
            AMQP.Queue.DeclareOk result = channel.queueDeclare(uuid + ".batchJob", true, false, false, null);
            String queueName = result.getQueue();
            channel.queueBind(queueName, "CSBatchJob", uuid);
            channel.queueBind(queueName, "CSBatchJob", HostMessage.ALL_HOSTS);
            batchJobConsumer = new RabbitQueueingConsumer(channel);
            channel.basicConsume(queueName, false, batchJobConsumer);
            batchControlConsumer = new RabbitMessageConsumer(channel, "CSBatchControl", uuid + ".batchControl",
                                                             Minion.this, routingKeys);
            return true;
        } catch (IOException e) {
            log.error("Error connecting to rabbitmq {}:{}", batchBrokerHost, batchBrokerPort, e);
            return false;
        }
    }

    static void shutdown() {
        System.exit(1);
    }

    void disconnectFromMQ() {
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
                        task.setAutoRetry(nextKick.getAutoRetry());
                        try {
                            task.exec(nextKick, true);
                        } catch (Exception ex) {
                            log.warn("[kick] exception while trying to kick {}", task.getName(), ex);
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
                    messageTaskExecutorService.execute(new CommandTaskStopRunner(Minion.this, core));
                    break;
                case CMD_TASK_REVERT:
                    messageTaskExecutorService.execute(new CommandTaskRevertRunner(Minion.this, core));
                    break;
                case CMD_TASK_DELETE:
                    messageTaskExecutorService.execute(new CommandTaskDeleteRunner(Minion.this, core));
                    break;
                case CMD_TASK_REPLICATE:
                    messageTaskExecutorService.execute(new CommandTaskReplicateRunner(Minion.this, core));
                    break;
                case CMD_TASK_PROMOTE_REPLICA:
                    // Legacy; ignore
                    break;
                case CMD_TASK_NEW:
                    messageTaskExecutorService.execute(new CommandCreateNewTask(Minion.this, core));
                    break;
                case CMD_TASK_DEMOTE_REPLICA:
                    // Legacy; ignore
                    break;
                case CMD_TASK_UPDATE_REPLICAS:
                    promoteDemoteTaskExecutorService.execute(new CommandTaskUpdateReplicasRunner(Minion.this, core));
                    break;
                case STATUS_TASK_JUMP_SHIP:
                    break;
                default:
                    log.warn("[mq.core] unhandled type = {}", core.getMessageType());
                    break;
            }
        }
    }

    /**
     * import/update jobs from a given root
     */
    int updateJobsMeta(File jobsRoot) throws IOException {
        int loaded = 0;
        File[] list = jobsRoot.isDirectory() ? jobsRoot.listFiles() : null;
        if (list == null || list.length == 0) {
            return 0;
        }
        for (File jobRoot : list) {
            loaded += updateJobMeta(jobRoot);
        }
        log.info("[import] {} tasks from directory '{}'", loaded, jobsRoot);
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
            log.warn("[task.update] invalid task root {}", taskRoot);
            return false;
        }
        JobKey key = new JobKey(jobID, taskID);
        JobTask task = tasks.get(key.toString());
        if (task == null) {
            task = new JobTask(Minion.this);
            tasks.put(key.toString(), task);
        }
        if (task.restoreTaskState(taskRoot)) {
            log.warn("[import.task] {} as {}", key, task.isRunning() ? "running" : "idle");
            return true;
        } else {
            log.warn("[import.task] {} failed", key);
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
            diskHealthCheck.onFailure();
        } finally {
            minionStateLock.unlock();
        }
    }

    private HostState createHostState() {
        long time = System.currentTimeMillis();
        HostState status = new HostState(uuid);
        status.setHost(myHost);
        if (useMesos) {
            status.setPort(mesosWebPort);
        } else {
            status.setPort(getJettyPort());
        }
        status.setGroup(group);
        status.setTime(time);
        status.setUser(user);
        status.setPath(path);
        status.setDiskReadOnly(diskReadOnly);
        status.setUptime(time - startTime);
        capacityLock.lock();
        try {
            int availSlots = maxActiveTasks - activeTaskKeys.size();
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
                log.warn("Failed to detect status of job {}; omitting from host state", job, ex);
            }

        }
        status.setRunning(running.toArray(new JobKey[running.size()]));
        status.setReplicating(replicating.toArray(new JobKey[replicating.size()]));
        status.setBackingup(backingUp.toArray(new JobKey[backingUp.size()]));
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
            this.zkClient = ZkUtil.makeStandardClient();
        }
        zkClient.getConnectionStateListenable().addListener((client, newState) -> {
            if (newState == ConnectionState.RECONNECTED) {
                joinGroup();
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
        log.info("joining group: {}", upPath);
        minionGroupMembership.addToGroup(upPath, getUUID(), shutdown);
    }

    void sendControlMessage(HostMessage msg) {
        synchronized (jmsxmitlock) {
            try {
                if (batchControlProducer != null) {
                    batchControlProducer.sendMessage(msg, msg.getHostUuid());
                }
            } catch (Exception ex) {
                log.warn("[mq.ctrl.send] fail", ex);
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
                log.warn("[mq.ctrl.send] fail", ex);
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
                    log.warn("[mq.ctrl.send] exception", ex);
                    if (i < sendStatusRetries - 1) {
                        log.warn("[mq.ctrl.send] fail on try {}; retrying", i);
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
                log.warn("[nextport] skipping port {}", nextPort, ex);
            }
        }
        return nextPort;
    }

    void removeJobFromQueue(JobKey key, boolean sendToSpawn) {
        minionStateLock.lock();
        try {
            for (Iterator<CommandTaskKick> iter = jobQueue.iterator(); iter.hasNext(); ) {
                CommandTaskKick kick = iter.next();
                if (kick.getJobKey().matches(key)) {
                    log.warn("[task.stop] removing from queue {} kick={} key={}", kick.getJobKey(), kick, key);
                    if (sendToSpawn) {
                        try {
                            sendStatusMessage(new StatusTaskEnd(uuid, kick.getJobUuid(), kick.getNodeID(), 0, 0, 0));
                        } catch (Exception ex) {
                            log.warn("---> send fail {} {} {}", uuid, key, kick, ex);
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
        JobTask task = new JobTask(Minion.this);
        task.id = jobID;
        task.node = node;
        task.taskRoot = new File(rootDir, task.id + "/" + task.node);
        log.warn("[task.new] restore {}/{} root={}", task.id, task.node, task.taskRoot);
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
                if (ProcessUtils.shell(MacUtils.rmcmd + " -rf " + file.getAbsolutePath(), rootDir) != 0) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean getShutdown() {
        return shutdown.get();
    }

    @Override public void close() throws Exception {
        jetty.stop();
        minionTaskDeleter.stopDeletionThread();
        if (zkClient != null && zkClient.getState() == CuratorFrameworkState.STARTED) {
            minionGroupMembership.removeFromGroup("/minion/up", getUUID());
            zkClient.close();
        }
    }
}

