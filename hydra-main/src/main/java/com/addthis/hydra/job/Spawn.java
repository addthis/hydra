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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

import java.net.InetAddress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import java.text.ParseException;

import com.addthis.basis.net.HttpUtil;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.RollingLog;
import com.addthis.basis.util.Strings;
import com.addthis.basis.util.TokenReplacerOverflowException;

import com.addthis.bark.StringSerializer;
import com.addthis.bark.ZkUtil;
import com.addthis.codec.Codec;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.job.backup.ScheduledBackupType;
import com.addthis.hydra.job.mq.CommandTaskDelete;
import com.addthis.hydra.job.mq.CommandTaskKick;
import com.addthis.hydra.job.mq.CommandTaskReplicate;
import com.addthis.hydra.job.mq.CommandTaskRevert;
import com.addthis.hydra.job.mq.CommandTaskStop;
import com.addthis.hydra.job.mq.CoreMessage;
import com.addthis.hydra.job.mq.HostCapacity;
import com.addthis.hydra.job.mq.HostMessage;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.mq.JobKey;
import com.addthis.hydra.job.mq.ReplicaTarget;
import com.addthis.hydra.job.mq.StatusTaskBackup;
import com.addthis.hydra.job.mq.StatusTaskBegin;
import com.addthis.hydra.job.mq.StatusTaskCantBegin;
import com.addthis.hydra.job.mq.StatusTaskEnd;
import com.addthis.hydra.job.mq.StatusTaskPort;
import com.addthis.hydra.job.mq.StatusTaskReplica;
import com.addthis.hydra.job.mq.StatusTaskReplicate;
import com.addthis.hydra.job.mq.StatusTaskRevert;
import com.addthis.hydra.job.spawn.JobAlert;
import com.addthis.hydra.job.spawn.JobAlertRunner;
import com.addthis.hydra.job.spawn.SpawnService;
import com.addthis.hydra.job.store.DataStoreUtil;
import com.addthis.hydra.job.store.JobStore;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.hydra.job.store.SpawnDataStoreKeys;
import com.addthis.hydra.query.spawndatastore.AliasBiMap;
import com.addthis.hydra.task.run.TaskExitState;
import com.addthis.hydra.util.DirectedGraph;
import com.addthis.hydra.util.EmailUtil;
import com.addthis.hydra.util.SettableGauge;
import com.addthis.hydra.util.WebSocketManager;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;

import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.job.store.SpawnDataStoreKeys.MINION_DEAD_PATH;
import static com.addthis.hydra.job.store.SpawnDataStoreKeys.MINION_UP_PATH;
import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_BALANCE_PARAM_PATH;
import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_COMMON_ALERT_PATH;
import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_COMMON_COMMAND_PATH;
import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_COMMON_MACRO_PATH;
import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_QUEUE_PATH;
import jsr166e.ConcurrentHashMapV8;

/**
 * manages minions running on remote notes. runs master http server to
 * communicate with and control those instances.
 */
public class Spawn implements Codec.Codable {

    private static final Logger log = LoggerFactory.getLogger(Spawn.class);
    private static final boolean meshQueue = Parameter.boolValue("queue.mesh", false);
    private static final boolean enableSpawn2 = Parameter.boolValue("spawn.v2.enable", true);
    private static final String httpHost = Parameter.value("spawn.localhost");
    private static final String clusterName = Parameter.value("cluster.name", "localhost");
    private static final String queryHttpHost = Parameter.value("spawn.queryhost");
    private static final int webPort = Parameter.intValue("spawn.http.port", 5050);
    private static final int requestHeaderBufferSize = Parameter.intValue("spawn.http.bufsize", 8192);
    private static final int hostStatusRequestInterval = Parameter.intValue("spawn.status.interval", 5_000);
    private static final int queueKickInterval = Parameter.intValue("spawn.queue.kick.interval", 3_000);
    private static final int backgroundThreads = Parameter.intValue("spawn.background.threads", 4);
    private static final int backgroundQueueSize = Parameter.intValue("spawn.background.queuesize", 1_000);
    private static final int backgroundHttpTimeout = Parameter.intValue("spawn.background.timeout", 300_000);

    private static final int backgroundEmailMinute = Parameter.intValue("spawn.background.notification.interval.minutes", 60);
    private static final String backgroundEmailAddress = Parameter.value("spawn.background.notification.address");
    public  static final long inputMaxNumberOfCharacters = Parameter.longValue("spawn.input.max.length", 1_000_000);
    private static final long MILLISECONDS_PER_MINUTE = (1_000 * 60);
    private static final AtomicLong emailLastFired = new AtomicLong();

    private static final BlockingQueue<Runnable> backgroundTaskQueue = new LinkedBlockingQueue<>(backgroundQueueSize);

    private static final ExecutorService backgroundService = MoreExecutors.getExitingExecutorService(
            new ThreadPoolExecutor(backgroundThreads, backgroundThreads, 0L, TimeUnit.MILLISECONDS,
                    backgroundTaskQueue), 100, TimeUnit.MILLISECONDS);
    private static String debugOverride = Parameter.value("spawn.debug");
    private static final boolean useStructuredLogger = Parameter.boolValue("spawn.logger.bundle.enable",
            clusterName.equals("localhost")); // default to true if-and-only-if we are running local stack
    private static final Codec codec = new CodecJSON();
    private static final Counter quiesceCount = Metrics.newCounter(Spawn.class, "quiesced");
    private static final SettableGauge<Integer> runningTaskCount = SettableGauge.newSettableGauge(Spawn.class, "runningTasks", 0);
    private static final SettableGauge<Integer> queuedTaskCount = SettableGauge.newSettableGauge(Spawn.class, "queuedTasks", 0);
    private static final SettableGauge<Integer> failTaskCount = SettableGauge.newSettableGauge(Spawn.class, "failedTasks", 0);
    private static final Meter tasksStartedPerHour = Metrics.newMeter(Spawn.class, "tasksStartedPerHour", "tasksStartedPerHour", TimeUnit.HOURS);
    private static final Meter tasksCompletedPerHour = Metrics.newMeter(Spawn.class, "tasksCompletedPerHour", "tasksCompletedPerHour", TimeUnit.HOURS);
    private static final SettableGauge<Integer> runningJobCount = SettableGauge.newSettableGauge(Spawn.class, "runningJobs", 0);
    private static final SettableGauge<Integer> queuedJobCount = SettableGauge.newSettableGauge(Spawn.class, "queuedJobs", 0);
    private static final SettableGauge<Integer> failJobCount = SettableGauge.newSettableGauge(Spawn.class, "failedJobs", 0);
    private static final SettableGauge<Integer> hungJobCount = SettableGauge.newSettableGauge(Spawn.class, "hungJobs", 0);
    private static final Meter jobsStartedPerHour = Metrics.newMeter(Spawn.class, "jobsStartedPerHour", "jobsStartedPerHour", TimeUnit.HOURS);
    private static final Meter jobsCompletedPerHour = Metrics.newMeter(Spawn.class, "jobsCompletedPerHour", "jobsCompletedPerHour", TimeUnit.HOURS);
    private static final Counter nonConsumingClientDropCounter = Metrics.newCounter(Spawn.class, "clientDrops");
    private static final Counter nonHostTaskMessageCounter = Metrics.newCounter(Spawn.class, "nonHostTaskMessage");
    private static final Meter jobTaskUpdateHeartbeatSuccessMeter = Metrics.newMeter(Spawn.class, "jobTaskUpdateHeartbeatSuccess", "jobTaskUpdateHeartbeatSuccess", TimeUnit.MINUTES);
    private static final Counter jobTaskUpdateHeartbeatFailureCounter = Metrics.newCounter(Spawn.class, "jobTaskUpdateHeartbeatFailure");
    private static final long JOB_TASK_UPDATE_HEARTBEAT_INTERVAL = Parameter.longValue("spawn.jobtask.update.interval", 30000);

    public static final String SPAWN_DATA_DIR = Parameter.value("SPAWN_DATA_DIR", "./data");
    public static final String SPAWN_STRUCTURED_LOG_DIR = Parameter.value("spawn.logger.bundle.dir", "./log/spawn-stats");

    private static final int clientDropTimeMillis = Parameter.intValue("spawn.client.drop.time", 60_000);
    private static final int clientDropQueueSize = Parameter.intValue("spawn.client.drop.queue", 2000);

    // thread pool for expanding jobs and sending kick messages (outside of the main application threads)
    // - thread pool size of 10 chosen somewhat arbitrarily, most job expansions should be nearly instantaneous
    // - max queue size of 5000 was chosen as a generous upper bound for how many tasks may be queued at once (since the number of scheduled kicks is limited by queue size)
    private final LinkedBlockingQueue<Runnable> expandKickQueue = new LinkedBlockingQueue<>(5000);
    private final ExecutorService expandKickExecutor = MoreExecutors.getExitingExecutorService(
            new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, expandKickQueue,
                    new ThreadFactoryBuilder().setNameFormat("jobExpander-%d").build()));
    private final ScheduledExecutorService scheduledExecutor = MoreExecutors.getExitingScheduledExecutorService(
            new ScheduledThreadPoolExecutor(4, new ThreadFactoryBuilder().setNameFormat("spawnScheduledTask-%d").build()));

    private final Gauge<Integer> expandQueueGauge = Metrics.newGauge(Spawn.class, "expandKickExecutorQueue", new Gauge<Integer>() {
        public Integer value() {
            return expandKickQueue.size();
        }
    });
    private final Gauge<Integer> backgroundQueueGauge = Metrics.newGauge(Spawn.class, "backgroundExecutorQueue", new Gauge<Integer>() {
        public Integer value() {
            return backgroundTaskQueue.size();
        }
    });
    private final HostFailWorker hostFailWorker;
    private final RollingLog eventLog;
    private static final boolean eventLogCompress = Parameter.boolValue("spawn.eventlog.compress", true);
    private static final int logMaxAge = Parameter.intValue("spawn.event.log.maxAge", 60 * 60 * 1000);
    private static final int logMaxSize = Parameter.intValue("spawn.event.log.maxSize", 100 * 1024 * 1024);
    private static final String logDir = Parameter.value("spawn.event.log.dir", "log");

    public static void main(String args[]) throws Exception {
        Spawn spawn = new Spawn(
                new File(args.length > 0 ? args[0] : "etc"),
                new File(args.length > 1 ? args[1] : "web")
        );
        if (enableSpawn2) new SpawnService(spawn).start();
    }

    private final File dataDir;
    private final ConcurrentHashMap<String, ClientEventListener> listeners;

    @Codec.Set(codable = true)
    private String uuid;
    @Codec.Set(codable = true)
    private String debug;
    @Codec.Set(codable = true)
    private String queryHost;
    @Codec.Set(codable = true)
    private String spawnHost;
    @Codec.Set(codable = true)
    private int queryPort = 2222;
    @Codec.Set(codable = true)
    private boolean quiesce;
    @Codec.Set(codable = true)
    private final HashSet<String> disabledHosts = new HashSet<>();
    private final int defaultReplicaCount = Parameter.intValue("spawn.defaultReplicaCount", 1);
    private static final int TASK_QUEUE_DRAIN_INTERVAL = Parameter.intValue("task.queue.drain.interval", 500);
    private static final boolean ENABLE_JOB_STORE = Parameter.boolValue("job.store.enable", true);
    private static final boolean ENABLE_JOB_FIXDIRS_ONCOMPLETE = Parameter.boolValue("job.fixdirs.oncomplete", true);


    private final ConcurrentHashMap<String, HostState> monitored;
    private final SpawnState spawnState = new SpawnState();
    private final SpawnMesh spawnMesh;
    private final SpawnFormattedLogger spawnFormattedLogger;

    private CuratorFramework zkClient;
    private SpawnMQ spawnMQ;
    private Server jetty;
    private JobConfigManager jobConfigManager;
    private SetMembershipListener minionMembers;
    private SetMembershipListener deadMinionMembers;
    private AliasBiMap aliasBiMap;
    private boolean useZk = true;
    private final String stateFilePath = Parameter.value("spawn.state.file", "spawn.state");
    private Gauge<Integer> minionsDown = Metrics.newGauge(Spawn.class, "minionsDown", new Gauge<Integer>() {
        public Integer value() {
            int total = 0;
            if (monitored != null) {
                synchronized (monitored) {
                    total = monitored.size();
                }
            }
            int up = minionMembers == null ? 0 : minionMembers.getMemberSetSize();
            return total - up;
        }
    });

    private SpawnBalancer balancer;
    private SpawnQueuesByPriority taskQueuesByPriority = new SpawnQueuesByPriority();
    private volatile int lastQueueSize = 0;
    private final Lock jobLock = new ReentrantLock();
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final LinkedBlockingQueue<String> jobUpdateQueue = new LinkedBlockingQueue<>();
    private final SpawnJobFixer spawnJobFixer = new SpawnJobFixer(this);
    private JobAlertRunner jobAlertRunner;
    private JobStore jobStore;
    private SpawnDataStore spawnDataStore;
    //To track web socket connections
    private final WebSocketManager webSocketManager = new WebSocketManager();

    /**
     * default constructor used for testing purposes only
     */

    @VisibleForTesting
    public Spawn() throws Exception {
        this(null);
    }

    @VisibleForTesting
    public Spawn(CuratorFramework zkClient) throws Exception {
        this.dataDir = Files.initDirectory(SPAWN_DATA_DIR);
        this.listeners = new ConcurrentHashMap<>();
        this.monitored = new ConcurrentHashMap<>();
        this.useZk = zkClient != null;
        this.spawnFormattedLogger = useStructuredLogger ?
                                    SpawnFormattedLogger.createFileBasedLogger(new File(SPAWN_STRUCTURED_LOG_DIR)) :
                                    SpawnFormattedLogger.createNullLogger();
        if (useZk) {
            log.info("[init] starting zkclient, config manager, and listening for minions");
            this.zkClient = zkClient;
            this.spawnDataStore = DataStoreUtil.makeCanonicalSpawnDataStore(true);
            this.jobConfigManager = new JobConfigManager(this.spawnDataStore);
            this.minionMembers = new SetMembershipListener(zkClient, MINION_UP_PATH);
            this.deadMinionMembers = new SetMembershipListener(zkClient, MINION_DEAD_PATH);
        }
        this.hostFailWorker = new HostFailWorker(this, scheduledExecutor);
        this.balancer = new SpawnBalancer(this);
        this.spawnMesh = new SpawnMesh(this);
        this.eventLog = new RollingLog(new File(logDir, "events-jobs"), "job", eventLogCompress, logMaxSize, logMaxAge);
    }

    private Spawn(File dataDir, File webDir) throws Exception {
        this.dataDir = Files.initDirectory(dataDir);
        File statefile = new File(dataDir, stateFilePath);
        if (statefile.exists() && statefile.isFile()) {
            codec.decode(this, Files.read(statefile));
        }
        getSettings().setQuiesced(quiesce);
        this.monitored = new ConcurrentHashMap<>();
        this.listeners = new ConcurrentHashMap<>();
        this.spawnFormattedLogger = useStructuredLogger ?
                                    SpawnFormattedLogger.createFileBasedLogger(new File(SPAWN_STRUCTURED_LOG_DIR)) :
                                    SpawnFormattedLogger.createNullLogger();
        this.zkClient = ZkUtil.makeStandardClient();
        this.spawnDataStore = DataStoreUtil.makeCanonicalSpawnDataStore(true);
        this.queryHost = (queryHttpHost != null ? queryHttpHost : InetAddress.getLocalHost().getHostAddress()) + ":" + queryPort;
        this.spawnHost = (httpHost != null ? httpHost : InetAddress.getLocalHost().getHostAddress()) + ":" + webPort;
        if (uuid == null) {
            uuid = UUID.randomUUID().toString();
            log.warn("[init] uuid was null, creating new one: " + uuid);
        }
        if (debugOverride != null) {
            debug = debugOverride;
        }
        // look for local object to import
        log.info("[init] beginning to load stats from data store");
        loadMacros();
        loadCommands();
        loadSpawnQueue();
        this.jobConfigManager = new JobConfigManager(spawnDataStore);
        // fix up null pointers
        for (Job job : spawnState.jobs.values()) {
            if (job.getSubmitTime() == null) {
                job.setSubmitTime(System.currentTimeMillis());
            }
        }
        loadJobs();
        // register jvm shutdown hook to clean up resources
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                runtimeShutdownHook();
            }
        });
        // connect to message broker or fail
        // connect to mesh
        this.spawnMesh = new SpawnMesh(this);
        log.info("[init] connecting to message queue");
        this.spawnMQ = meshQueue ? new SpawnMQImplMesh(zkClient, this) : new SpawnMQImpl(zkClient, this);
        this.minionMembers = new SetMembershipListener(zkClient, MINION_UP_PATH);
        this.deadMinionMembers = new SetMembershipListener(zkClient, MINION_DEAD_PATH);
        this.aliasBiMap = new AliasBiMap(spawnDataStore);
        aliasBiMap.loadCurrentValues();
        hostFailWorker = new HostFailWorker(this, scheduledExecutor);
        balancer = new SpawnBalancer(this);
        loadSpawnBalancerConfig();
        this.spawnMQ.connectToMQ(uuid);

        //Start JobAlertManager
        this.jobAlertRunner = new JobAlertRunner(this);
        // start job scheduler
        scheduledExecutor.scheduleWithFixedDelay(new UpdateEventRunnable(), 0, 1, TimeUnit.MINUTES);
        scheduledExecutor.scheduleWithFixedDelay(new JobRekickTask(), 0, 500, TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                drainJobTaskUpdateQueue();
            }
        }, TASK_QUEUE_DRAIN_INTERVAL, TASK_QUEUE_DRAIN_INTERVAL, TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                jobTaskUpdateHeartbeatCheck();
            }
        }, JOB_TASK_UPDATE_HEARTBEAT_INTERVAL, JOB_TASK_UPDATE_HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
        // request hosts to send their status
        scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                requestHostsUpdate();
            }
        }, hostStatusRequestInterval, hostStatusRequestInterval, TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                kickJobsOnQueue();
                writeSpawnQueue();
            }
        }, queueKickInterval, queueKickInterval, TimeUnit.MILLISECONDS);
        // start http commands listener(s)
        startSpawnWeb(dataDir, webDir);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    jetty.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        balancer.startAutobalanceTask();
        balancer.startTaskSizePolling();
        if (ENABLE_JOB_STORE) {
            jobStore = new JobStore(new File(dataDir, "jobstore"));
        }
        this.eventLog = new RollingLog(new File(logDir, "events-jobs"), "job", eventLogCompress, logMaxSize, logMaxAge);
    }

    private void writeState() {
        try {
            Files.write(new File(dataDir, stateFilePath), codec.encode(this), false);
        } catch (Exception e) {
            log.warn("WARNING: failed to write spawn state to log file at " + stateFilePath);
        }

    }

    public void markHostsForFailure(String hostId, HostFailWorker.FailState failState) {
        hostFailWorker.markHostsToFail(hostId, failState);
    }

    public void markHostsForFailure(String hostId, boolean deadFileSystem) {
        markHostsForFailure(hostId, deadFileSystem ? HostFailWorker.FailState.FAILING_FS_DEAD : HostFailWorker.FailState.FAILING_FS_OKAY);
    }

    public void unmarkHostsForFailure(String hostIds) {
        hostFailWorker.removeHostsForFailure(hostIds);
    }

    public HostFailWorker getHostFailWorker() {
        return hostFailWorker;
    }

    public SpawnBalancer getSpawnBalancer() {
        return balancer;
    }

    public static String getHttpHost() {
        return httpHost;
    }

    public void acquireJobLock() {
        jobLock.lock();
    }

    public void releaseJobLock() {
        jobLock.unlock();
    }

    private void startSpawnWeb(File dataDir, File webDir) throws Exception {
        log.info("[init] starting http server");
        SpawnHttp http = new SpawnHttp(this, webDir);
        new SpawnManager().register(http);
        jetty = new Server(webPort);
        jetty.getConnectors()[0].setRequestBufferSize(65535);
        jetty.getConnectors()[0].setRequestHeaderSize(requestHeaderBufferSize);
        jetty.setHandler(http);
        jetty.start();
    }

    public String getUuid() {
        return uuid;
    }

    public MeshyClient getMeshyClient() {
        spawnMesh.waitLinkUp();
        return spawnMesh.getClient();
    }

    public CuratorFramework getZkClient() {
        return zkClient;
    }

    private void closeZkClients() {
        if (spawnDataStore != null) {
            spawnDataStore.close();
        }
        if (zkClient != null) {
            zkClient.close();
        }
    }


    public void setSpawnMQ(SpawnMQ spawnMQ) {
        this.spawnMQ = spawnMQ;
    }

    private void loadMacros() throws Exception {
        Map<String, String> loadedMacros = spawnDataStore.getAllChildren(SPAWN_COMMON_MACRO_PATH);
        if (loadedMacros == null) {
            return;
        }
        for (Entry<String, String> macroEntry : loadedMacros.entrySet()) {
            String jsonMacro = macroEntry.getValue();
            if (jsonMacro != null && !jsonMacro.equals("null") && !jsonMacro.isEmpty()) {
                JobMacro macro = new JobMacro();
                codec.decode(macro, jsonMacro.getBytes());
                putMacro(macroEntry.getKey(), macro, false);
            }
        }
    }

    // TODO: It should be possible to reduce duplication between how commands and macros are handled.
    @VisibleForTesting
    protected void loadCommands() throws Exception {
        Map<String, String> loadedCommands = spawnDataStore.getAllChildren(SPAWN_COMMON_COMMAND_PATH);
        if (loadedCommands == null) {
            return;
        }
        for (Entry<String, String> commandEntry : loadedCommands.entrySet()) {
            String jsonCommand = commandEntry.getValue();
            if (jsonCommand != null && !jsonCommand.equals("null") && !jsonCommand.isEmpty()) {
                JobCommand command = new JobCommand();
                codec.decode(command, jsonCommand.getBytes());
                putCommand(commandEntry.getKey(), command, false);
            }
        }
    }

    @VisibleForTesting
    protected void loadSpawnQueue() throws Exception {
        String queueFromZk = spawnDataStore.get(SPAWN_QUEUE_PATH);
        if (queueFromZk == null) {
            return;
        }
        try {
            taskQueuesByPriority = new ObjectMapper().readValue(queueFromZk, SpawnQueuesByPriority.class);
        } catch (Exception ex) {
            log.warn("[task.queue] exception during spawn queue deserialization: ", ex);
        }
    }

    protected void writeSpawnQueue() {
        ObjectMapper om = new ObjectMapper();
        try {
            taskQueuesByPriority.lock();
            try {
                spawnDataStore.put(SPAWN_QUEUE_PATH, new String(om.writeValueAsBytes(taskQueuesByPriority)));
            } finally {
                taskQueuesByPriority.unlock();
            }
        } catch (Exception ex) {
            log.warn("[task.queue] exception during spawn queue serialization: " + ex, ex);
        }
    }

    @VisibleForTesting
    protected void loadJobs() {
        if (jobConfigManager != null) {
            jobLock.lock();
            try {
                for (IJob iJob : jobConfigManager.getJobs().values()) {
                    if (iJob != null) {
                        putJobInSpawnState(new Job(iJob));
                    }
                }
            } finally {
                jobLock.unlock();
            }
        }
        Thread loadDependencies = new Thread() {
            @Override
            public void run() {
                Set<String> jobIds = spawnState.jobs.keySet();
                for (String jobId : jobIds) {
                    IJob job = getJob(jobId);
                    if (job != null) {
                        updateJobDependencies(job);
                    }
                }
            }
        };
        loadDependencies.setDaemon(true);
        loadDependencies.start();
    }

    // -------------------- BEGIN API ---------------------

    public Settings getSettings() {
        return new Settings();
    }


    public Map<String, List<String>> getAliases() {
        return aliasBiMap.viewAliasMap();
    }

    public List<String> aliasToJobs(String alias) {
        return aliasBiMap.getJobs(alias);
    }

    public void addAlias(String alias, List<String> jobs) {
        if (jobs.size() > 0) {
            aliasBiMap.putAlias(alias, jobs);
        } else {
            log.warn("Ignoring empty jobs addition for alias: " + alias);
        }
    }

    public void deleteAlias(String alias) {
        aliasBiMap.deleteAlias(alias);
    }

    public ClientEventListener getClientEventListener(String id) {
        ClientEventListener listener = listeners.get(id);
        if (listener == null) {
            listener = new ClientEventListener();
            listeners.put(id, listener);
        }
        listener.lastSeen = System.currentTimeMillis();
        return listener;
    }

    public HostState getHostState(String hostUuid) {
        if (hostUuid == null) {
            return null;
        }
        synchronized (monitored) {
            return monitored.get(hostUuid);
        }
    }

    public HostState markHostStateDead(String hostUUID) {
        HostState state = getHostState(hostUUID);
        if (state != null) {
            state.setDead(true);
            state.setUpdated();
            if (useZk) {
                // delete minion state
                spawnDataStore.delete(Minion.MINION_ZK_PATH + hostUUID);
                try {
                    zkClient.create().creatingParentsIfNeeded().forPath(MINION_DEAD_PATH + "/" + hostUUID, null);
                } catch (KeeperException.NodeExistsException ne) {
                    // host already marked as dead
                } catch (Exception e) {
                    log.error("Unable to add host: " + hostUUID + " to " + MINION_DEAD_PATH);
                }
            }
            sendHostUpdateEvent(state);
            updateHostState(state);
        }
        return state;
    }

    protected void updateHostState(HostState state) {
        synchronized (monitored) {
            if (deadMinionMembers == null || !deadMinionMembers.getMemberSet().contains(state.getHostUuid())) {
                if (log.isDebugEnabled()) {
                    log.debug("Updating host state for : " + state.getHost());
                }
                monitored.put(state.getHostUuid(), state);
            }
        }
    }

    /**
     * List all hosts belonging to a particular minion type.
     *
     * @param minionType The minion type to find. If null, return all hosts.
     * @return A list of hoststates
     */
    public List<HostState> listHostStatus(String minionType) {
        synchronized (monitored) {
            Set<String> availableMinions = minionMembers == null ? ImmutableSet.<String>of() : minionMembers.getMemberSet();
            Set<String> deadMinions = deadMinionMembers == null ? ImmutableSet.<String>of() : deadMinionMembers.getMemberSet();
            ArrayList<HostState> allMinions = new ArrayList<>();
            for (HostState minion : monitored.values()) {
                if (availableMinions.contains(minion.getHostUuid()) && !deadMinions.contains(minion.getHostUuid())) {
                    minion.setUp(true);
                } else {
                    minion.setUp(false);
                }
                if (minionType == null || minion.hasType(minionType)) {
                    allMinions.add(minion);
                }
            }
            return allMinions;
        }
    }

    public Collection<String> listAvailableHostIds() {
        return minionMembers.getMemberSet();
    }


    public void requestHostsUpdate() {
        try {
            spawnMQ.sendControlMessage(new HostState(HostMessage.ALL_HOSTS));
        } catch (Exception e) {
            log.warn("unable to request host state update: ", e);
        }
    }

    public Set<String> getDataSources(String jobId) {
        HashSet<String> dataSources = new HashSet<>();
        Job job = this.getJob(jobId);
        if (job == null || job.getParameters() == null) {
            return dataSources;
        }
        jobLock.lock();
        try {
            for (JobParameter param : job.getParameters()) {
                String value = param.getValue();
                if (Strings.isEmpty(value)) {
                    value = param.getDefaultValue();
                }
                if (value != null) {
                    try {
                        value = JobExpand.macroExpand(this, value);
                    } catch (TokenReplacerOverflowException ex) {
                        log.error("Token replacement overflow for input '" + value + "'");
                    }
                }
                if (value != null && spawnState.jobs.containsKey(value)) {
                    dataSources.add(value);
                }
            }
        } finally {
            jobLock.unlock();
        }
        return dataSources;
    }

    public DirectedGraph<String> getJobDependencies() {
        return spawnState.jobDependencies;
    }

    //* returns the jobs that depend on a given job. dependency is established if the job's ID is used as a job parameter
    public Collection<Job> listDependentJobs(String jobId) {
        ArrayList<Job> dependents = new ArrayList<>();
        jobLock.lock();
        try {
            for (Job job : spawnState.jobs.values()) {
                for (JobParameter param : job.getParameters()) {
                    if (param.getValue() != null && param.getValue().equals(jobId)) {
                        dependents.add(job);
                        break;
                    }
                }
            }
            return dependents;
        } finally {
            jobLock.unlock();
        }
    }
    /**
     * Gets the backup times for a given job and node of all backup types by using MeshyClient. If the nodeId is -1 it will
     * get the backup times for all nodes.
     *
     * @return Set of date time mapped by backup type in reverse chronological order
     * @throws IOException thrown if mesh client times out, ParseException thrown if filename does not meet valid format
     */
    public Map<ScheduledBackupType, SortedSet<Long>> getJobBackups(String jobUUID, int nodeId) throws IOException, ParseException {
        Map<ScheduledBackupType, SortedSet<Long>> fileDates = new HashMap<ScheduledBackupType, SortedSet<Long>>();
        for (ScheduledBackupType backupType : ScheduledBackupType.getBackupTypes().values()) {
            final String typePrefix = "*/" + jobUUID + "/" + ((nodeId < 0) ? "*" : Integer.toString(nodeId)) + "/" + backupType.getPrefix() + "*";
            List<FileReference> files = new ArrayList<FileReference>(spawnMesh.getClient().listFiles(new String[]{typePrefix}));
            fileDates.put(backupType, new TreeSet<Long>(Collections.reverseOrder()));
            for (FileReference file : files) {
                String filename = file.name.split("/")[4];
                fileDates.get(backupType).add(backupType.parseDateFromName(filename).getTime());
            }
        }
        return fileDates;
    }

    public boolean isSpawnMeshAvailable() {
        return spawnMesh.getClient() != null;
    }

    public void deleteHost(String hostuuid) {
        HostFailWorker.FailState failState = hostFailWorker.getFailureState(hostuuid);
        if (failState == HostFailWorker.FailState.FAILING_FS_DEAD || failState == HostFailWorker.FailState.FAILING_FS_OKAY) {
            log.warn("Refused to drop host because it was in the process of being failed", hostuuid);
            throw new RuntimeException("Cannot drop a host that is in the process of being failed");
        }
        synchronized (monitored) {
            HostState state = monitored.remove(hostuuid);
            if (state != null) {
                log.info("Deleted host " + hostuuid);
                sendHostUpdateEvent("host.delete", state);
            } else {
                log.warn("Attempted to delete host " + hostuuid + "But it was not found");
            }
        }
    }

    public Collection<Job> listJobs() {
        ArrayList<Job> clones = new ArrayList<>(spawnState.jobs.size());
        jobLock.lock();
        try {
            for (Job job : spawnState.jobs.values()) {
                clones.add(job);
            }
            return clones;
        } finally {
            jobLock.unlock();
        }
    }

    public Collection<Job> listJobsConcurrentImmutable() {
        return Collections.unmodifiableCollection(spawnState.jobs.values());
    }

    public JSONArray getTaskQueueAsJSONArray() {
        taskQueuesByPriority.lock();
        try {
            JSONArray jsonArray = new JSONArray();
            for (Integer priority : taskQueuesByPriority.keySet()) {
                Map<String, Object> jobToTaskMap = new HashMap<>();
                LinkedList<SpawnQueueItem> jobQueue = taskQueuesByPriority.get(priority);
                for (JobKey jobkey : jobQueue) {
                    JobTask jobtask = getTask(jobkey.getJobUuid(), jobkey.getNodeNumber());

                    String hostStr = "";
                    hostStr += jobtask.getHostUUID() + " ";
                    for (JobTaskReplica jobTaskReplica : jobtask.getReplicas()) {
                        hostStr += jobTaskReplica.getHostUUID() + " ";
                    }

                    HashMap<String, Object> taskHostMap = (HashMap<String, Object>) jobToTaskMap.get(jobkey.getJobUuid());
                    if (taskHostMap == null) {
                        taskHostMap = new HashMap<>();
                    }

                    taskHostMap.put(Integer.toString(jobtask.getTaskID()), hostStr);
                    jobToTaskMap.put(jobkey.getJobUuid(), taskHostMap);
                }
                JSONObject jobResult = new JSONObject(jobToTaskMap);
                jsonArray.put(jobResult);
            }
            return jsonArray;
        } finally {
            taskQueuesByPriority.unlock();
        }
    }

    public int getTaskQueuedCount() {
        return lastQueueSize;
    }

    public Job getJob(String jobUUID) {
        if (jobUUID == null) {
            return null;
        }
        jobLock.lock();
        try {
            return spawnState.jobs.get(jobUUID);
        } finally {
            jobLock.unlock();
        }
    }

    public void setJobConfig(String jobUUID, String config) throws Exception {
        jobConfigManager.setConfig(jobUUID, config);
    }

    public String getJobConfig(String jobUUID) {
        if (jobUUID == null) {
            return null;
        }
        jobLock.lock();
        try {
            return jobConfigManager.getConfig(jobUUID);
        } finally {
            jobLock.unlock();
        }
    }

    public Job putJobInSpawnState(Job job) {
        if (job == null) {
            return null;
        }
        // Null out the job config before inserting to reduce the amount stored in memory.
        // Calling getJob will fill it back in -- or call jobConfigManager.getConfig(id)
        job.setConfig(null);
        return spawnState.jobs.put(job.getId(), job);
    }

    public Job getJob(JobKey jobKey) {
        String jobUUID = jobKey.getJobUuid();
        return getJob(jobUUID);
    }

    public JSONArray getJobHistory(String jobId) {
        return jobStore != null ? jobStore.getHistory(jobId) : new JSONArray();
    }

    public String getJobHistoricalConfig(String jobId, String commitId) {
        return jobStore != null ? jobStore.fetchHistoricalConfig(jobId, commitId) : null;
    }

    public String diff(String jobId, String commitId) {
        return jobStore != null ? jobStore.getDiff(jobId, commitId) : null;
    }

    public Job createJob(String creator, int taskCount, Collection<String> taskHosts, String minionType, String command) throws Exception {
        jobLock.lock();
        try {
            Job job = new Job(UUID.randomUUID().toString(), creator != null ? creator : "anonymous");
            job.setOwner(job.getCreator());
            job.setState(JobState.IDLE);
            job.setCommand(command);
            job.setDailyBackups(4);
            job.setWeeklyBackups(1);
            job.setMonthlyBackups(0);
            job.setHourlyBackups(0);
            job.setReplicas(defaultReplicaCount);
            job.setMinionType(minionType);
            List<HostState> hostStates = getOrCreateHostStateList(minionType, taskHosts);
            List<JobTask> tasksAssignedToHosts = balancer.generateAssignedTasksForNewJob(job.getId(), taskCount, hostStates);
            job.setTasks(tasksAssignedToHosts);
            for (JobTask task : tasksAssignedToHosts) {
                HostState host = getHostState(task.getHostUUID());
                if (host == null) {
                    throw new Exception("Unable to allocate job tasks because no suitable host was found");
                }
                host.addJob(job.getId());
            }
            putJobInSpawnState(job);
            if (jobConfigManager != null) {
                jobConfigManager.addJob(job);
            }
            submitConfigUpdate(job.getId(), null);
            return job;
        } finally {
            jobLock.unlock();
        }
    }

    public boolean synchronizeJobState(String jobUUID) {
        if (jobUUID == null) {
            throw new NullPointerException("missing job uuid");
        }
        if (jobUUID.equals("ALL")) {
            Collection<Job> jobList = listJobs();
            for (Job job : jobList) {
                if (!synchronizeSingleJob(job.getId())) {
                    log.warn("Stopping synchronize all jobs to to failure synchronizing job: " + job.getId());
                    return false;
                }
            }
            return true;
        } else {
            return synchronizeSingleJob(jobUUID);
        }
    }

    private boolean synchronizeSingleJob(String jobUUID) {
        Job job = getJob(jobUUID);
        if (job == null) {
            log.warn("[job.synchronize] job uuid " + jobUUID + " not found");
            return false;
        }
        ObjectMapper mapper = new ObjectMapper();
        for (JobTask task : job.getCopyOfTasks()) {
            String taskHost = task.getHostUUID();
            if (deadMinionMembers.getMemberSet().contains(taskHost)) {
                log.warn("task is currently assigned to a dead minion, need to check job: " + job.getId() + " host/node:" + task.getHostUUID() + "/" + task.getTaskID());
                continue;
            }
            String hostStateString;
            try {
                hostStateString = StringSerializer.deserialize(zkClient.getData().forPath(Minion.MINION_ZK_PATH + taskHost));
            } catch (Exception e) {
                log.error("Unable to get hostStateString from zookeeper for " + Minion.MINION_ZK_PATH + taskHost, e);
                continue;
            }
            HostState hostState;
            try {
                hostState = mapper.readValue(hostStateString, HostState.class);
            } catch (IOException e) {
                log.warn("Unable to deserialize host state for host: " + hostStateString + " serialized string was\n" + hostStateString);
                return false;
            }
            boolean matched = matchJobNodeAndId(jobUUID, task, hostState.getRunning(), hostState.getStopped(), hostState.getQueued());
            if (!matched) {
                log.warn("Spawn thinks job: " + jobUUID + " node:" + task.getTaskID() + " is running on host: " + hostState.getHost() + " but that host disagrees.");
                if (matchJobNodeAndId(jobUUID, task, hostState.getReplicas())) {
                    log.warn("Host: " + hostState.getHost() + " has a replica for the task/node: " + jobUUID + "/" + task.getTaskID() + " promoting replica");
                    try {
                        rebalanceReplicas(job);
                    } catch (Exception e) {
                        log.warn("Exception promoting replica during job synchronization on host: " + taskHost + " job/node" + job.getId() + "/" + job.getId());
                    }
                } else {
                    log.warn("Host: " + hostState.getHost() + " does NOT have a replica for the task/node: " + jobUUID + "/" + task.getTaskID());
                }
            } else {
                log.warn("Spawn and minion agree, job/node: " + jobUUID + "/" + task.getTaskID() + " is on host: " + hostState.getHost());
            }
        }
        return true;
    }

    private boolean matchJobNodeAndId(String jobUUID, JobTask task, JobKey[]... jobKeys) {
        for (JobKey[] jobKeyArray : jobKeys) {
            for (JobKey jobKey : jobKeyArray) {
                if (jobKey == null) {
                    log.warn("runningJob was null, this shouldn't happen");
                    continue;
                } else if (jobKey.getJobUuid() == null) {
                    log.warn("JobUUID for jobKey: " + jobKey + " was null");
                    continue;
                } else if (jobKey.getNodeNumber() == null) {
                    log.warn("NodeNumber for jobKey: " + jobKey + " was null");
                    continue;
                }
                if (jobKey.getJobUuid().equals(jobUUID) && jobKey.getNodeNumber().equals(task.getTaskID())) {
                    return true;
                }
            }
        }

        return false;
    }

    public List<HostState> getLiveHostsByReadOnlyStatus(String minionType, boolean readonly) {
        List<HostState> allHosts = listHostStatus(minionType);
        List<HostState> rv = new ArrayList<>(allHosts.size());
        for (HostState host : allHosts) {
            if (host.isUp() && !host.isDead() && host.isReadOnly() == readonly) {
                rv.add(host);
            }
        }
        return rv;
    }

    /**
     * Reallocate some of a job's tasks to different hosts, hopefully improving its performance.
     *
     * @param jobUUID     The ID of the job
     * @param tasksToMove The number of tasks to move. If <= 0, use the default.
     * @return a list of move assignments that were attempted
     */
    public List<JobTaskMoveAssignment> reallocateJob(String jobUUID, int tasksToMove, boolean readonly) {
        Job job;
        if (jobUUID == null || (job = getJob(jobUUID)) == null) {
            throw new NullPointerException("invalid job uuid");
        }
        if (job.getState() != JobState.IDLE) {
            log.warn("[job.reallocate] can't reallocate non-idle job");
            return null;
        }
        List<JobTaskMoveAssignment> assignments = balancer.getAssignmentsForJobReallocation(job, tasksToMove, getLiveHostsByReadOnlyStatus(job.getMinionType(), readonly));
        return executeReallocationAssignments(assignments, false);
    }

    /**
     * Promote a task to live on one of its replica hosts, demoting the existing live to a replica.
     *
     * @param task           The task to modify
     * @param replicaHostID  The host holding the replica that should be promoted
     * @param kickOnComplete Whether to kick the task after the move is complete
     * @return true on success
     */
    public boolean swapTask(JobTask task, String replicaHostID, boolean kickOnComplete) {
        if (task == null) {
            log.warn("[task.swap] received null task");
            return false;
        }
        if (!checkHostStatesForSwap(task.getJobKey(), task.getHostUUID(), replicaHostID, true)) {
            log.warn("[swap.task.stopped] failed for " + task.getJobKey() + "; exiting");
            return false;
        }
        Job job;
        jobLock.lock();
        try {
            job = getJob(task.getJobUUID());
            task.replaceReplica(replicaHostID, task.getHostUUID());
            task.setHostUUID(replicaHostID);
            queueJobTaskUpdateEvent(job);
        } finally {
            jobLock.unlock();
        }
        if (kickOnComplete) {
            try {
                scheduleTask(job, task, expandJob(job));
            } catch (Exception e) {
                log.warn("Warning: failed to kick task " + task.getJobKey() + " with: " + e, e);
                job.errorTask(task, JobTaskErrorCode.KICK_ERROR);
            }
        }
        return true;
    }

    /**
     * Get a replacement host for a new task
     *
     * @param job The job for the task to be reassigned
     * @return A replacement host ID, if one can be found; null otherwise
     */
    private String getReplacementHost(Job job) {
        List<HostState> hosts = getLiveHostsByReadOnlyStatus(job.getMinionType(), false);
        for (HostState host : hosts) {
            if (host.canMirrorTasks()) {
                return host.getHostUuid();
            }
        }
        return null;
    }

    /**
     * Given a new task, replace any hosts that are down/disabled to ensure that it can kick
     *
     * @param task The task to modify
     * @return True if at least one host was removed
     */
    private boolean replaceDownHosts(JobTask task) {
        Job job = getJob(task.getJobKey());
        if (job == null) {
            return false;
        }
        HostState host = getHostState(task.getHostUUID());
        boolean changed = false;
        if (host == null || !host.canMirrorTasks()) {
            String replacementHost = getReplacementHost(job);
            if (replacementHost != null) {
                task.setHostUUID(replacementHost);
                changed = true;
            }
        }
        if (task.getReplicas() != null) {
            List<JobTaskReplica> tempReplicas = new ArrayList<>(task.getReplicas());
            for (JobTaskReplica replica : tempReplicas) {
                HostState replicaHost = getHostState(replica.getHostUUID());
                if (replicaHost == null || !replicaHost.canMirrorTasks()) {
                    changed = true;
                    task.setReplicas(removeReplicasForHost(replica.getHostUUID(), task.getReplicas()));
                }
            }
        }
        if (changed) {
            try {
                updateJob(job);
            } catch (Exception ex) {
                log.warn("Failed to sent replication message for new task " + task.getJobKey() + ": " + ex, ex);
                return false;
            }
        }
        return changed;

    }

    /**
     * Check whether it is acceptable to swap a task between two hosts
     *
     * @param key           The task to consider swapping
     * @param liveHostID    The current host for the task
     * @param replicaHostID The potential target host to check
     * @return True if both hosts are up and have the appropriate task directory
     */
    private boolean checkHostStatesForSwap(JobKey key, String liveHostID, String replicaHostID, boolean checkTargetReplica) {
        if (key == null || liveHostID == null || replicaHostID == null) {
            log.warn("[task.swap] failed due to null input");
            return false;
        }
        JobTask task = getTask(key.getJobUuid(), key.getNodeNumber());
        if (task == null) {
            log.warn("[task.swap] failed: nonexistent task/replicas");
            return false;
        }
        HostState liveHost = getHostState(liveHostID);
        HostState replicaHost = getHostState(replicaHostID);
        if (liveHost == null || replicaHost == null || liveHost.isDead() || !liveHost.isUp() || replicaHost.isDead() || !replicaHost.isUp()) {
            log.warn("[task.swap] failed due to invalid host states for " + liveHostID + "," + replicaHostID);
            return false;
        }
        if (checkTargetReplica && !isNewTask(task)) {
            if (!replicaHost.hasLive(key)) {
                log.warn("[task.swap] failed because the replica host " + replicaHostID + " does not have a complete replica of task " + key);
                return false;
            }
        }
        return true;
    }

    /**
     * Push or pull tasks off of a host to balance its load with the rest of the cluster.
     *
     * @param hostUUID The ID of the host
     * @return a boolean describing if at least one task was scheduled to be moved
     */
    public RebalanceOutcome rebalanceHost(String hostUUID) {
        if (hostUUID == null || !monitored.containsKey(hostUUID)) {
            return new RebalanceOutcome(hostUUID, "missing host", null, null);
        }
        HostState host = monitored.get(hostUUID);
        boolean readOnly = host.isReadOnly();
        log.warn("[job.reallocate] starting reallocation for host: " + hostUUID + " host is " + (readOnly ? "" : "not") + " a read only host");
        List<JobTaskMoveAssignment> assignments = balancer.getAssignmentsToBalanceHost(host, getLiveHostsByReadOnlyStatus(null, host.isReadOnly()));
        return new RebalanceOutcome(hostUUID, null, null, Strings.join(executeReallocationAssignments(assignments, false).toArray(), "\n"));
    }

    /**
     * Sanity-check a series of task move assignments coming from SpawnBalancer, then execute the sensible ones.
     *
     * @param assignments The assignments to execute
     * @param limitToAvailableSlots Whether movements should honor their host's availableTaskSlots count
     * @return The number of tasks that were actually moved
     */
    public List<JobTaskMoveAssignment> executeReallocationAssignments(List<JobTaskMoveAssignment> assignments, boolean limitToAvailableSlots) {
        List<JobTaskMoveAssignment> executedAssignments = new ArrayList<>();
        if (assignments == null) {
            return executedAssignments;
        }
        HashSet<String> jobsNeedingUpdate = new HashSet<>();
        HashSet<String> hostsAlreadyMovingTasks = new HashSet<>();
        for (JobTaskMoveAssignment assignment : assignments) {
            if (assignment.delete()) {
                log.warn("[job.reallocate] deleting " + assignment.getJobKey() + " off " + assignment.getSourceUUID());
                deleteTask(assignment.getJobKey().getJobUuid(), assignment.getSourceUUID(), assignment.getJobKey().getNodeNumber(), false);
                deleteTask(assignment.getJobKey().getJobUuid(), assignment.getSourceUUID(), assignment.getJobKey().getNodeNumber(), true);
                executedAssignments.add(assignment);
            } else {
                String sourceHostID = assignment.getSourceUUID();
                String targetHostID = assignment.getTargetUUID();
                HostState targetHost = getHostState(targetHostID);
                if (sourceHostID == null || targetHostID == null || sourceHostID.equals(targetHostID) || targetHost == null) {
                    log.warn("[job.reallocate] received invalid host assignment: from " + sourceHostID + " to " + targetHostID);
                    continue;
                }
                JobKey key = assignment.getJobKey();
                JobTask task = getTask(key);
                Job job = getJob(key);
                if (job == null || task == null) {
                    log.warn("[job.reallocate] invalid job or task");
                    // Continue with the next assignment
                }
                else {
                    HostState liveHost = getHostState(task.getHostUUID());
                    if (limitToAvailableSlots && liveHost != null && (liveHost.getAvailableTaskSlots() == 0 || hostsAlreadyMovingTasks.contains(task.getHostUUID()))) {
                        continue;
                    }
                    log.warn("[job.reallocate] replicating task " + key + " onto " + targetHostID + " as " + (assignment.isFromReplica() ? "replica" : "live"));
                    TaskMover tm = new TaskMover(this, key, targetHostID, sourceHostID);
                    if (tm.execute()) {
                        hostsAlreadyMovingTasks.add(task.getHostUUID());
                        executedAssignments.add(assignment);
                    }
                }
            }
        }
        for (String jobUUID : jobsNeedingUpdate) {
            try {
                updateJob(getJob(jobUUID));
            } catch (Exception ex) {
                log.warn("WARNING: failed to update job " + jobUUID + ": " + ex, ex);
            }
        }
        return executedAssignments;
    }

    /**
     * A method to ensure all live/replicas exist where they should, and optimize their locations if all directories are correct
     *
     * @param jobUUID     The job id to rebalance
     * @param tasksToMove The number of tasks to move. If < 0, use the default.
     * @return a RebalanceOutcome describing which steps were performed
     * @throws Exception If there is a failure when rebalancing replicas
     */
    public RebalanceOutcome rebalanceJob(String jobUUID, int tasksToMove) throws Exception {
        Job job = getJob(jobUUID);
        if (jobUUID == null || job == null) {
            log.warn("[job.rebalance] job uuid " + jobUUID + " not found");
            return new RebalanceOutcome(jobUUID, "job not found", null, null);
        }
        if (job.getState() != JobState.IDLE && job.getState() != JobState.DEGRADED) {
            log.warn("[job.rebalance] job must be IDLE or DEGRADED to rebalance " + jobUUID);
            return new RebalanceOutcome(jobUUID, "job not idle/degraded", null, null);
        }
        // First, make sure each task has claimed all the replicas it should have
        if (!rebalanceReplicas(job)) {
            log.warn("[job.rebalance] failed to fill out replica assignments for " + jobUUID);
            return new RebalanceOutcome(jobUUID, "couldn't fill out replicas", null, null);
        }
        try {
            List<JobTaskDirectoryMatch> allMismatches = new ArrayList<>();
            // Check each task to see if any live/replica directories are missing or incorrectly placed
            for (JobTask task : job.getCopyOfTasks()) {
                if (replaceDownHosts(task)) {
                    continue;
                }
                List<JobTaskDirectoryMatch> directoryMismatches = matchTaskToDirectories(task, false);
                if (!directoryMismatches.isEmpty()) {
                    // If there are issues with a task's directories, resolve them.
                    resolveJobTaskDirectoryMatches(task, false);
                    allMismatches.addAll(directoryMismatches);
                }
            }
            updateJob(job);
            // If any mismatches were found, skip the optimization step
            if (!allMismatches.isEmpty()) {
                return new RebalanceOutcome(jobUUID, null, Strings.join(allMismatches.toArray(), "\n"), null);
            } else {
                // If all tasks had all expected directories, consider moving some tasks to better hosts
                return new RebalanceOutcome(jobUUID, null, null, Strings.join(reallocateJob(jobUUID, tasksToMove, false).toArray(), "\n"));
            }
        } catch (Exception ex) {
            log.warn("[job.rebalance] exception during rebalance for " + jobUUID, ex);
            return new RebalanceOutcome(jobUUID, "exception during rebalancing: " + ex, null, null);
        }

    }

    /**
     * For a particular task, ensure all live/replica copies exist where they should
     *
     * @param jobId           The job id to fix
     * @param node            The task id to fix, or -1 to fix all
     * @param ignoreTaskState Whether to ignore the task's state (mostly when recovering from a host failure)
     * @param orphansOnly     Whether to only delete orphans for idle tasks
     * @return A string description
     */
    public JSONObject fixTaskDir(String jobId, int node, boolean ignoreTaskState, boolean orphansOnly) {
        jobLock.lock();
        try {
            Job job = getJob(jobId);
            int numChanged = 0;
            if (job != null) {
                List<JobTask> tasks = node < 0 ? job.getCopyOfTasks() : Arrays.asList(job.getTask(node));
                for (JobTask task : tasks) {
                    boolean shouldModifyTask = !spawnJobFixer.haveRecentlyFixedTask(task.getJobKey()) &&
                                               (ignoreTaskState || (task.getState() == JobTaskState.IDLE || (!orphansOnly && task.getState() == JobTaskState.ERROR)));
                    if (log.isDebugEnabled()) {
                        log.debug("[fixTaskDir] considering modifying task " + task.getJobKey() + " shouldModifyTask=" + shouldModifyTask);
                    }
                    if (shouldModifyTask) {
                        try {
                            numChanged += resolveJobTaskDirectoryMatches(task, orphansOnly) ? 1 : 0;
                            spawnJobFixer.markTaskRecentlyFixed(task.getJobKey());
                        } catch (Exception ex) {
                            log.warn("fixTaskDir exception " + ex, ex);
                        }
                    }
                }
            }
            return new JSONObject(ImmutableMap.of("tasksChanged", numChanged));
        } finally {
            jobLock.unlock();
        }

    }

    /**
     * Go through the hosts in the cluster, making sure that a task has copies everywhere it should and doesn't have orphans living elsewhere
     * @param task The task to examine
     * @param deleteOrphansOnly Whether to ignore missing copies and only delete orphans
     * @return True if the task was changed
     */
    public boolean resolveJobTaskDirectoryMatches(JobTask task, boolean deleteOrphansOnly) {
        Set<String> expectedHostsWithTask = new HashSet<>();
        Set<String> expectedHostsMissingTask = new HashSet<>();
        Set<String> unexpectedHostsWithTask = new HashSet<>();
        replaceDownHosts(task);
        for (HostState host : listHostStatus(null)) {
            if (hostSuitableForReplica(host)) {
                String hostId = host.getHostUuid();
                if (hostId.equals(task.getHostUUID()) || task.hasReplicaOnHost(hostId)) {
                    if (host.hasLive(task.getJobKey())) {
                        expectedHostsWithTask.add(hostId);
                    } else {
                        expectedHostsMissingTask.add(hostId);
                    }
                } else if (host.hasLive(task.getJobKey())) {
                    unexpectedHostsWithTask.add(hostId);
                }
            }
        }
        log.trace("fixTaskDirs found expectedWithTask {} expectedMissingTask {} unexpectedWithTask {} ", expectedHostsWithTask, expectedHostsMissingTask, unexpectedHostsWithTask);
        if (deleteOrphansOnly) {
            // If we're only deleting orphans, ignore any expected hosts missing the task
            expectedHostsMissingTask = new HashSet<>();
        }
        return performTaskFixes(task, expectedHostsWithTask, expectedHostsMissingTask, unexpectedHostsWithTask);
    }

    private boolean performTaskFixes(JobTask task, Set<String> expectedHostsWithTask, Set<String> expectedHostsMissingTask, Set<String> unexpectedHostsWithTask) {
        if (expectedHostsWithTask.isEmpty()) {
            // No copies of the task were found on the expected live/replica hosts. Attempt to recover other copies from the cluster.
            if (unexpectedHostsWithTask.isEmpty()) {
                // No copies of the task were found anywhere in the cluster. Have to recreate it.
                log.warn("No copies of {} were found. Recreating it on new hosts. ", task.getJobKey());
                recreateTask(task);
                return true;
            }
            // Found at least one host with data. Iterate through the hosts with data; first host becomes live, any others become replicas
            Iterator<String> unexpectedHostsIter = unexpectedHostsWithTask.iterator();
            List<JobTaskReplica> newReplicas = new ArrayList<>();
            task.setHostUUID(unexpectedHostsIter.next());
            while (unexpectedHostsIter.hasNext()) {
                newReplicas.add(new JobTaskReplica(unexpectedHostsIter.next(), task.getJobUUID(), 0, 0));
            }
            task.setReplicas(newReplicas);
            return true;
        } else {
            // Found copies of task on expected hosts. Copy to any hosts missing the data, and delete from any unexpected hosts
            boolean changed = false;
            if (!expectedHostsMissingTask.isEmpty()) {
                swapTask(task, expectedHostsWithTask.iterator().next(), false);
                copyTaskToReplicas(task);
                changed = true;
            }
            for (String unexpectedHost : unexpectedHostsWithTask) {
                deleteTask(task.getJobUUID(), unexpectedHost, task.getTaskID(), false);
            }
            return changed;
        }
    }

    private boolean hostSuitableForReplica(HostState host) {
        return host != null && host.isUp() && !host.isDead();
    }

    private void copyTaskToReplicas(JobTask task) {
        sendControlMessage(new CommandTaskReplicate(task.getHostUUID(), task.getJobUUID(), task.getTaskID(), getTaskReplicaTargets(task, task.getReplicas()), null, null, false, false));
    }

    private void recreateTask(JobTask task) {
        Job job = getJob(task.getJobUUID());
        Map<JobTask, String> assignmentMap = balancer.assignTasksFromMultipleJobsToHosts(Arrays.asList(task), getOrCreateHostStateList(job.getMinionType(), null));
        if (assignmentMap != null && assignmentMap.containsKey(task)) {
            String newHostUUID = assignmentMap.get(task);
            log.warn("[job.rebalance] assigning new host for " + task.getJobUUID() + ":" + task.getTaskID() + " all data on previous host will be lost");
            task.setHostUUID(newHostUUID);
            task.resetTaskMetrics();
        } else {
            log.warn("[job.rebalance] unable to assign new host for " + task.getJobUUID() + ":" + task.getTaskID() + " could not find suitable host");
        }
    }

    public String checkTaskDirText(String jobId, int node) {
        jobLock.lock();
        try {
            Job job = getJob(jobId);
            if (job == null) {
                return "NULL JOB";
            }
            StringBuilder sb = new StringBuilder();
            List<JobTask> tasks = node < 0 ? new ArrayList<>(job.getCopyOfTasksSorted()) : Arrays.asList(job.getTask(node));
            sb.append("Directory check for job ").append(job.getId()).append("\n");
            for (JobTask task : tasks) {
                sb.append("Task ").append(task.getTaskID()).append(": ").append(matchTaskToDirectories(task, true)).append("\n");
            }
            return sb.toString();
        } finally {
            jobLock.unlock();
        }
    }

    public JSONArray checkTaskDirJSON(String jobId, int node) {
        JSONArray resultList = new JSONArray();
        jobLock.lock();
        try {
            Job job = getJob(jobId);
            if (job == null) {
                return resultList;
            }
            List<JobTask> tasks = node < 0 ? new ArrayList<>(job.getCopyOfTasksSorted()) : Arrays.asList(job.getTask(node));
            for (JobTask task : tasks) {
                List<JobTaskDirectoryMatch> taskMatches = matchTaskToDirectories(task, true);
                for (JobTaskDirectoryMatch taskMatch : taskMatches) {
                    JSONObject jsonObject = CodecJSON.encodeJSON(taskMatch);
                    resultList.put(jsonObject);
                }
            }
        } catch (Exception ex) {
            log.warn("Error: checking dirs for job: " + jobId + ", node: " + node);
        } finally {
            jobLock.unlock();
        }
        return resultList;
    }

    public List<JobTaskDirectoryMatch> matchTaskToDirectories(JobTask task, boolean includeCorrect) {
        List<JobTaskDirectoryMatch> rv = new ArrayList<>();
        JobTaskDirectoryMatch match = checkHostForTask(task, task.getHostUUID());
        if (includeCorrect || match.getType() != JobTaskDirectoryMatch.MatchType.MATCH) {
            rv.add(match);
        }
        if (task.getAllReplicas() != null) {
            for (JobTaskReplica replica : task.getAllReplicas()) {
                match = checkHostForTask(task, replica.getHostUUID());
                if (match.getType() != JobTaskDirectoryMatch.MatchType.MATCH) {
                    if (task.getState() == JobTaskState.REPLICATE || task.getState() == JobTaskState.FULL_REPLICATE) {
                        // If task is replicating, it will temporarily look like it's missing on the target host. Make this visible to the UI.
                        rv.add(new JobTaskDirectoryMatch(JobTaskDirectoryMatch.MatchType.REPLICATE_IN_PROGRESS, match.getJobKey(), match.getHostId()));
                    } else {
                        rv.add(match);
                    }
                }
                else if (includeCorrect) {
                    rv.add(match);
                }
            }
        }
        rv.addAll(findOrphansForTask(task));
        return rv;
    }

    private JobTaskDirectoryMatch checkHostForTask(JobTask task, String hostID) {
        JobTaskDirectoryMatch.MatchType type;
        HostState host = getHostState(hostID);
        if (host == null || !host.hasLive(task.getJobKey())) {
            type = JobTaskDirectoryMatch.MatchType.MISMATCH_MISSING_LIVE;
        } else {
            type = JobTaskDirectoryMatch.MatchType.MATCH;
        }
        return new JobTaskDirectoryMatch(type, task.getJobKey(), hostID);
    }

    private List<JobTaskDirectoryMatch> findOrphansForTask(JobTask task) {
        List<JobTaskDirectoryMatch> rv = new ArrayList<>();
        Job job = getJob(task.getJobUUID());
        if (job == null) {
            log.warn("got find orphans request for missing job " + task.getJobUUID());
            return rv;
        }
        Set<String> expectedTaskHosts = task.getAllTaskHosts();
        for (HostState host : listHostStatus(job.getMinionType())) {
            if (host == null || !host.isUp() || host.isDead() || host.getHostUuid().equals(task.getRebalanceTarget())) {
                continue;
            }
            if (!expectedTaskHosts.contains(host.getHostUuid())) {
                JobTaskDirectoryMatch.MatchType type = null;
                if (host.hasLive(task.getJobKey()) || host.hasIncompleteReplica(task.getJobKey())) {
                    type = JobTaskDirectoryMatch.MatchType.ORPHAN_LIVE;
                }
                if (type != null) {
                    rv.add(new JobTaskDirectoryMatch(type, task.getJobKey(), host.getHostUuid()));
                }
            }
        }
        return rv;
    }

    private class TaskMover {

        /**
         * This class moves a task from a source host to a target host.
         * If the target host already had a replica of the task, that
         * replica is removed so the task will make a new replica somewhere
         * else.
         */
        private final JobKey taskKey;
        private final String targetHostUUID;
        private final String sourceHostUUID;
        private HostState targetHost;
        private Job job;
        private JobTask task;
        private boolean kickOnComplete;
        private boolean isMigration;
        private final Spawn spawn;

        TaskMover(Spawn spawn, JobKey taskKey, String targetHostUUID, String sourceHostUUID) {
            this.spawn = spawn;
            this.taskKey = taskKey;
            this.targetHostUUID = targetHostUUID;
            this.sourceHostUUID = sourceHostUUID;
        }

        public void setMigration(boolean isMigration) {
            this.isMigration = isMigration;
        }

        public String choreWatcherKey() {
            return targetHostUUID + "&&&" + taskKey;
        }

        public boolean execute() {
            targetHost = spawn.getHostState(targetHostUUID);
            if (taskKey == null || !spawn.checkStatusForMove(targetHostUUID) || !spawn.checkStatusForMove(sourceHostUUID)) {
                log.warn("[task.mover] erroneous input; terminating for: " + taskKey);
                return false;
            }
            job = spawn.getJob(taskKey);
            task = job.getTask(taskKey.getNodeNumber());
            if (task == null) {
                log.warn("[task.mover] failed to find job or task for: " + taskKey);
                return false;
            }
            HostState liveHost = spawn.getHostState(task.getHostUUID());
            if (liveHost == null || !liveHost.hasLive(task.getJobKey())) {
                log.warn("[task.mover] failed to find live task for: " + taskKey);
                fixTaskDir(taskKey.getJobUuid(), taskKey.getNodeNumber(), false, false);
                return false;
            }
            if (!task.getHostUUID().equals(sourceHostUUID) && !task.hasReplicaOnHost(sourceHostUUID)) {
                log.warn("[task.mover] failed because the task does not have a copy on the specified source: " + taskKey);
                return false;
            }
            if (task.getAllTaskHosts().contains(targetHostUUID) || targetHost.hasLive(taskKey)) {
                log.warn("[task.mover] cannot move onto a host with an existing version of task: " + taskKey);
                return false;
            }
            if (!targetHost.getMinionTypes().contains(job.getMinionType())) {
                log.warn("[task.mover] cannot move onto a host that lacks the appropriate minion type: " + taskKey);
                return false;
            }
            // If the task was rebalanced out of queued state, kick it again when the rebalance completes.
            kickOnComplete = task.getState() == JobTaskState.QUEUED || task.getState() == JobTaskState.QUEUED_HOST_UNAVAIL;
            if (!spawn.prepareTaskStatesForRebalance(job, task, isMigration)) {
                log.warn("[task.mover] couldn't set task states; terminating for: " + taskKey);
                return false;
            }
            // Swap to the lightest host to run the rsync operation, assuming swapping is allowed for this job.
            HostState lightestExistingHost = taskQueuesByPriority.findBestHostToRunTask(getHealthyHostStatesHousingTask(task, !job.getDontAutoBalanceMe()),false);
            if (lightestExistingHost != null && !lightestExistingHost.getHostUuid().equals(task.getHostUUID())) {
                swapTask(task, lightestExistingHost.getHostUuid(), false);
            }
            try {
                task.setRebalanceSource(sourceHostUUID);
                task.setRebalanceTarget(targetHostUUID);
                startReplicate();
                taskQueuesByPriority.markHostTaskActive(task.getHostUUID());
                queueJobTaskUpdateEvent(job);
                return true;
            } catch (Exception ex) {
                log.warn("[task.mover] exception during replicate initiation; terminating for task: " + taskKey, ex);
                task.setErrorCode(JobTaskErrorCode.EXIT_REPLICATE_FAILURE);
                task.setState(JobTaskState.ERROR);
                queueJobTaskUpdateEvent(job);
                return false;
            }
        }

        private void startReplicate() throws Exception {
            ReplicaTarget[] target = new ReplicaTarget[]{
                    new ReplicaTarget(
                            targetHostUUID,
                            targetHost.getHost(),
                            targetHost.getUser(),
                            targetHost.getPath(),
                            task.getReplicationFactor())
            };
            job.setSubmitCommand(getCommand(job.getCommand()));
            JobCommand jobcmd = job.getSubmitCommand();
            CommandTaskReplicate replicate = new CommandTaskReplicate(
                    task.getHostUUID(), task.getJobUUID(), task.getTaskID(), target, Strings.join(jobcmd.getCommand(), " "), choreWatcherKey(), true, kickOnComplete);
            replicate.setRebalanceSource(sourceHostUUID);
            replicate.setRebalanceTarget(targetHostUUID);
            spawn.sendControlMessage(replicate);
            log.warn("[task.mover] replicating job/task " + task.getJobKey() + " from " + sourceHostUUID + " onto host " + targetHostUUID);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("TaskMover");
            sb.append("{taskKey=").append(taskKey);
            sb.append(", targetHostUUID='").append(targetHostUUID).append('\'');
            sb.append(", sourceHostUUID='").append(sourceHostUUID).append('\'');
            sb.append(", job=").append(job);
            sb.append(", task=").append(task);
            sb.append(", kickOnComplete=").append(kickOnComplete);
            sb.append('}');
            return sb.toString();
        }
    }

    public boolean checkStatusForMove(String hostID) {
        HostState host = getHostState(hostID);
        if (host == null) {
            log.warn("[host.status] received null host for id " + hostID);
            return false;
        }
        if (host.isDead() || !host.isUp()) {
            log.warn("[host.status] host is down: " + hostID);
            return false;
        }
        return true;
    }

    public boolean prepareTaskStatesForRebalance(Job job, JobTask task, boolean isMigration) {
        jobLock.lock();
        try {
            if (!balancer.isInMovableState(task)) {
                log.warn("[task.mover] decided not to move non-idle task " + task);
                return false;
            }
            JobTaskState newState = isMigration ? JobTaskState.MIGRATING : JobTaskState.REBALANCE;
            job.setTaskState(task, newState, true);
            queueJobTaskUpdateEvent(job);
            return true;
        } finally {
            jobLock.unlock();
        }
    }

    /**
     * exclude failed hosts from eligible pool
     * iterate over tasks
     * assemble hosts job spread across
     * count replicas per host
     * iterate over tasks and make reductions
     * iterate over tasks and make additions
     * exclude task host from replica
     * assign in order of least replicas per host
     * <p/>
     * TODO synchronize on job
     * TODO allow all cluster hosts to be considered for replicas
     * TODO consider host group "rack aware" keep 1/first replica in same group
     *
     * @return true if rebalance was successful
     */
    public boolean rebalanceReplicas(Job job) throws Exception {
        // perform read/write and read only replication
        return rebalanceReplicas(job, false) && rebalanceReplicas(job, true);
    }

    /**
     * exclude failed hosts from eligible pool
     * iterate over tasks
     * assemble hosts job spread across
     * count replicas per host
     * iterate over tasks and make reductions
     * iterate over tasks and make additions
     * exclude task host from replica
     * assign in order of least replicas per host
     * <p/>
     * TODO synchronize on job
     * TODO allow all cluster hosts to be considered for replicas
     * TODO consider host group "rack aware" keep 1/first replica in same group
     *
     * @param job      the job to rebalance replicas
     * @param taskID   The task # to fill out replicas, or -1 for all tasks
     * @param readOnly Whether to fill out readonly replicas or standard replicas
     * @return true if rebalance was successful
     */
    public boolean rebalanceReplicas(Job job, int taskID, boolean readOnly) throws Exception {
        if (job == null) {
            return false;
        }
        boolean success = true;
        // Ensure that there aren't any replicas pointing towards the live host or duplicate replicas
        balancer.removeInvalidReplicas(job, readOnly);
        // Ask SpawnBalancer where new replicas should be sent
        Map<Integer, List<String>> replicaAssignments = balancer.getAssignmentsForNewReplicas(job, taskID, readOnly);
        List<JobTask> tasks = taskID > 0 ? Arrays.asList(job.getTask(taskID)) : job.getCopyOfTasks();
        for (JobTask task : tasks) {
            List<String> replicasToAdd = replicaAssignments.get(task.getTaskID());
            // Make the new replicas as dictated by SpawnBalancer
            if (readOnly) {
                task.setReadOnlyReplicas(addReplicasAndRemoveExcess(task, replicasToAdd, job.getReadOnlyReplicas(), task.getReadOnlyReplicas()));
            } else {
                task.setReplicas(addReplicasAndRemoveExcess(task, replicasToAdd, job.getReplicas(), task.getReplicas()));
            }
        }
        if (!readOnly) {
            success = validateReplicas(job);
        }
        return success;
    }

    public boolean rebalanceReplicas(Job job, boolean readOnly) throws Exception {
        return rebalanceReplicas(job, -1, readOnly);
    }

    /**
     * check all tasks. If there are still not enough replicas, record failure.
     *
     * @param job - the job to validate
     * @return true if the job has met its replica requirements
     */
    private boolean validateReplicas(Job job) {
        for (JobTask task : job.getCopyOfTasks()) {
            List<JobTaskReplica> replicas = task.getReplicas();
            if (job.getReplicas() > 0) {
                if (replicas == null || replicas.size() < job.getReplicas()) {
                    HostState currHost = getHostState(task.getHostUUID());
                    if ((currHost == null || currHost.isDead()) && (replicas == null || replicas.size() == 0)) // If current host is dead and there are no replicas, mark degraded
                    {
                        job.setState(JobState.DEGRADED);
                    } else {
                        job.setState(JobState.ERROR); // Otherwise, just mark errored so we will know that at least on replica failed
                        job.setEnabled(false);
                    }
                    log.warn("[replica.add] ERROR - unable to replicate task because there are not enough suitable hosts, job: " + job.getId());
                    return false;
                }
            }
        }
        return true;
    }

    private List<JobTaskReplica> addReplicasAndRemoveExcess(JobTask task, List<String> replicaHostsToAdd,
            int desiredNumberOfReplicas,
            List<JobTaskReplica> currentReplicas) throws Exception {
        List<JobTaskReplica> newReplicas = (currentReplicas == null ? new ArrayList<JobTaskReplica>() : new ArrayList<>(currentReplicas));
        if (replicaHostsToAdd != null) {
            newReplicas.addAll(replicateTask(task, replicaHostsToAdd));
        }
        if (!isNewTask(task)) {
            while (newReplicas.size() > desiredNumberOfReplicas) {
                JobTaskReplica replica = newReplicas.remove(newReplicas.size() - 1);
                spawnMQ.sendControlMessage(new CommandTaskDelete(replica.getHostUUID(), task.getJobUUID(), task.getTaskID(), task.getRunCount()));
                log.warn("[replica.delete] " + task.getJobUUID() + "/" + task.getTaskID() + " from " + replica.getHostUUID() + " @ " + getHostState(replica.getHostUUID()).getHost());
            }
        }
        return newReplicas;
    }

    protected List<JobTaskReplica> replicateTask(JobTask task, List<String> targetHosts) {
        List<JobTaskReplica> newReplicas = new ArrayList<>();
        for (String targetHostUUID : targetHosts) {
            JobTaskReplica replica = new JobTaskReplica();
            replica.setHostUUID(targetHostUUID);
            replica.setJobUUID(task.getJobUUID());
            newReplicas.add(replica);
        }
        Job job = getJob(task.getJobUUID());
        JobCommand jobcmd = job.getSubmitCommand();
        String command = (jobcmd != null && jobcmd.getCommand() != null) ? Strings.join(jobcmd.getCommand(), " ") : null;
        spawnMQ.sendControlMessage(new CommandTaskReplicate(task.getHostUUID(), task.getJobUUID(), task.getTaskID(), getTaskReplicaTargets(task, newReplicas), command, null, false, false));
        log.warn("[replica.add] " + task.getJobUUID() + "/" + task.getTaskID() + " to " + targetHosts);
        taskQueuesByPriority.markHostTaskActive(task.getHostUUID());
        return newReplicas;
    }


    private void updateJobDependencies(IJob job) {
        String jobId = job.getId();
        DirectedGraph<String> dependencies = spawnState.jobDependencies;
        Set<String> sources = dependencies.getSourceEdges(jobId);
        if (sources != null) {
            for (String source : sources) {
                dependencies.removeEdge(source, jobId);
            }
        } else {
            dependencies.addNode(jobId);
        }
        Set<String> newSources = this.getDataSources(jobId);
        if (newSources != null) {
            for (String source : newSources) {
                dependencies.addEdge(source, jobId);
            }
        }
    }

    /**
     * Submit a config update to the job store
     *
     * @param jobId         The job to submit
     * @param commitMessage If specified, the commit message to use
     */
    public void submitConfigUpdate(String jobId, String commitMessage) {
        Job job;
        if (jobId == null || jobId.isEmpty() || (job = getJob(jobId)) == null) {
            return;
        }
        if (jobStore != null) {
            jobStore.submitConfigUpdate(job.getId(), job.getOwner(), getJobConfig(jobId), commitMessage);
        }
    }

    public void updateJob(IJob ijob) throws Exception {
        updateJob(ijob, true);
    }

    /**
     * requires 'job' to be a different object from the one in cache.  make sure
     * to clone() any job fetched from cache before submitting to updateJob().
     */
    public void updateJob(IJob ijob, boolean reviseReplicas) throws Exception {
        if (useZk) {
            Job job = new Job(ijob);
            jobLock.lock();
            try {
                require(getJob(job.getId()) != null, "job " + job.getId() + " does not exist");
                updateJobDependencies(job);
                Job oldjob = putJobInSpawnState(job);
                // take action on trigger changes (like # replicas)
                if (oldjob != job && reviseReplicas) {
                    int oldReplicaCount = oldjob.getReplicas();
                    int newReplicaCount = job.getReplicas();
                    require(oldReplicaCount == newReplicaCount || job.getState() == JobState.IDLE || job.getState() == JobState.DEGRADED, "job must be IDLE or DEGRADED to change replicas");
                    require(newReplicaCount < monitored.size(), "replication factor must be < # live hosts");
                    rebalanceReplicas(job);
                }
                queueJobTaskUpdateEvent(job);
            } finally {
                jobLock.unlock();
            }
        }
    }

    public void putAlert(String alertId, JobAlert alert) {
        jobAlertRunner.putAlert(alertId, alert);
    }

    public void removeAlert(String alertId) {
        jobAlertRunner.removeAlert(alertId);
    }

    public JSONArray fetchAllAlertsArray() {
        return jobAlertRunner.getAlertStateArray();
    }

    public JSONObject fetchAllAlertsMap() {
        return jobAlertRunner.getAlertStateMap();
    }

    public String getAlert(String alertId) {
        return jobAlertRunner.getAlert(alertId);
    }

    public static enum DeleteStatus {
        SUCCESS, JOB_MISSING, JOB_DO_NOT_DELETE
    }

    public DeleteStatus deleteJob(String jobUUID) throws Exception {
        jobLock.lock();
        try {
            Job job = getJob(jobUUID);
            if (job == null) {
                return DeleteStatus.JOB_MISSING;
            }
            if (job.getDontDeleteMe()) {
                return DeleteStatus.JOB_DO_NOT_DELETE;
            }
            spawnState.jobs.remove(jobUUID);
            spawnState.jobDependencies.removeNode(jobUUID);
            log.warn("[job.delete] " + job.getId());
            spawnMQ.sendControlMessage(new CommandTaskDelete(HostMessage.ALL_HOSTS, job.getId(), null, job.getRunCount()));
            sendJobUpdateEvent("job.delete", job);
            if (jobConfigManager != null) {
                jobConfigManager.deleteJob(job.getId());
            }
            if (jobStore != null) {
                jobStore.delete(jobUUID);
            }
            Job.logJobEvent(job, JobEvent.DELETE, eventLog);
            return DeleteStatus.SUCCESS;
        } finally {
            jobLock.unlock();
        }
    }

    public void sendControlMessage(HostMessage hostMessage) {
        spawnMQ.sendControlMessage(hostMessage);
    }

    /**
     * Deletes a job only a specific host, useful when there are replicas and
     * a job has been migrated to another host
     *
     * @param jobUUID   The job to delete
     * @param hostUuid  The host where the delete message should be sent
     * @param node      The specific task to be deleted
     * @param isReplica Whether the task to be deleted is a replica or a live
     * @return True if the task is successfully removed
     */
    public boolean deleteTask(String jobUUID, String hostUuid, Integer node, boolean isReplica) {
        jobLock.lock();
        try {
            if (jobUUID == null || node == null) {
                return false;
            }
            log.warn("[job.delete.host] " + hostUuid + "/" + jobUUID + " >> " + node);
            spawnMQ.sendControlMessage(new CommandTaskDelete(hostUuid, jobUUID, node, 0));
            Job job = getJob(jobUUID);
            if (isReplica && job != null) {
                JobTask task = job.getTask(node);
                task.setReplicas(removeReplicasForHost(hostUuid, task.getReplicas()));
                task.setReadOnlyReplicas(removeReplicasForHost(hostUuid, task.getReadOnlyReplicas()));
                queueJobTaskUpdateEvent(job);
            }
            return true;
        } finally {
            jobLock.unlock();
        }
    }

    private List<JobTaskReplica> removeReplicasForHost(String hostUuid, List<JobTaskReplica> currentReplicas) {
        if (currentReplicas == null || currentReplicas.size() == 0) {
            return new ArrayList<>();
        }
        List<JobTaskReplica> replicasCopy = new ArrayList<>(currentReplicas);
        Iterator<JobTaskReplica> iterator = replicasCopy.iterator();
        while (iterator.hasNext()) {
            JobTaskReplica replica = iterator.next();
            if (replica.getHostUUID().equals(hostUuid)) {
                iterator.remove();
            }
        }
        return replicasCopy;
    }

    /**
     * The entry point for requests to start every task from a job (for example, from the UI.)
     *
     * @param jobUUID      Job ID
     * @param isManualKick Whether the task came from the interface, which is given special treatment during quiesce
     * @throws Exception
     */
    public void startJob(String jobUUID, boolean isManualKick) throws Exception {
        Job job = getJob(jobUUID);
        require(job != null, "job not found");
        require(job.isEnabled(), "job disabled");
        require(scheduleJob(job, isManualKick), "unable to schedule job");
        queueJobTaskUpdateEvent(job);
        Job.logJobEvent(job, JobEvent.START, eventLog);
    }

    public String expandJob(String jobUUID) throws Exception {
        Job job = getJob(jobUUID);
        require(job != null, "job not found");
        return expandJob(job);
    }

    public String expandJob(Job job) throws TokenReplacerOverflowException {
        return expandJob(job.getId(), job.getParameters(), getJobConfig(job.getId()));
    }

    public boolean moveTask(JobKey jobKey, String sourceUUID, String targetUUID) {
        if (sourceUUID == null || targetUUID == null || sourceUUID.equals(targetUUID)) {
            log.warn("[task.move] fail: invalid input " + sourceUUID + "," + targetUUID);
            return false;
        }
        TaskMover tm = new TaskMover(this, jobKey, targetUUID, sourceUUID);
        log.warn("[task.move] attempting move for " + jobKey);
        return tm.execute();
    }

    public String expandJob(String id, Collection<JobParameter> parameters, String rawConfig)
            throws TokenReplacerOverflowException {
        // macro recursive expansion
        String pass0 = JobExpand.macroExpand(this, rawConfig);
        // template in params that "may" contain other macros
        String pass1 = JobExpand.macroTemplateParams(pass0, parameters);
        // macro recursive expansion again
        String pass2 = JobExpand.macroExpand(this, pass1);
        // replace remaining params not caught in pass 1
        String pass3 = JobExpand.macroTemplateParams(pass2, parameters);
        // inject job metadata from spawn
        return JobExpand.magicMacroExpand(this, pass3, id);
    }

    public void stopJob(String jobUUID) throws Exception {
        Job job = getJob(jobUUID);
        require(job != null, "job not found");
        for (JobTask task : job.getCopyOfTasks()) {
            if (task.getState() == JobTaskState.QUEUED) {
                removeFromQueue(task);
            }
            stopTask(jobUUID, task.getTaskID());
        }
        job.setHadMoreData(false);
        Job.logJobEvent(job, JobEvent.STOP, eventLog);
    }

    public void killJob(String jobUUID) throws Exception {
        boolean success = false;
        while (!success & !shuttingDown.get()) {
            try {
                jobLock.lock();
                if (taskQueuesByPriority.tryLock()) {
                    success = true;
                    Job job = getJob(jobUUID);
                    Job.logJobEvent(job, JobEvent.KILL, eventLog);
                    require(job != null, "job not found");
                    for (JobTask task : job.getCopyOfTasks()) {
                        if (task.getState() == JobTaskState.QUEUED) {
                            removeFromQueue(task);
                        }
                        killTask(jobUUID, task.getTaskID());
                    }
                    job.setHadMoreData(false);
                }
            } finally {
                jobLock.unlock();
                if (success) {
                    taskQueuesByPriority.unlock();
                }
            }
        }
    }

    /**
     * not a clone like jobs, because there is no updater.
     * yes, there is no clean symmetry here.  it could use cleanup.
     */
    public JobTask getTask(String jobUUID, int taskID) {
        Job job = getJob(jobUUID);
        if (job != null) {
            return job.getTask(taskID);
        }
        return null;
    }

    public JobTask getTask(JobKey jobKey) {
        if (jobKey == null || jobKey.getJobUuid() == null || jobKey.getNodeNumber() == null) {
            return null;
        }
        return getTask(jobKey.getJobUuid(), jobKey.getNodeNumber());
    }

    /**
     * The entry point for requests to start tasks (for example, from the UI.) Does some checking, and ultimately
     * kicks the task or adds it to the task queue as appropriate
     *
     * @param jobUUID      Job ID
     * @param taskID       Node #
     * @param addToQueue   Whether the task should be added to the queue (false if the task is already on the queue)
     * @param isManualKick Whether the task came from the interface, which is given special treatment during quiesce
     * @param toQueueHead  Whether to add the task to the head of the queue rather than the end
     * @throws Exception When the task is invalid or already active
     */
    public void startTask(String jobUUID, int taskID, boolean addToQueue, boolean isManualKick, boolean toQueueHead) throws Exception {
        Job job = getJob(jobUUID);
        require(job != null, "job not found");
        require(job.isEnabled(), "job is disabled");
        require(job.getState() != JobState.DEGRADED, "job in degraded state");
        require(taskID >= 0, "invalid task id");
        JobTask task = getTask(jobUUID, taskID);
        require(task != null, "no such task");
        require(task.getState() != JobTaskState.BUSY && task.getState() != JobTaskState.ALLOCATED &&
                task.getState() != JobTaskState.QUEUED, "invalid task state");
        if (addToQueue) {
            addToTaskQueue(task.getJobKey(), isManualKick && quiesce, toQueueHead);
        } else {
            kickIncludingQueue(job, task, expandJob(job), false, isManualKick && quiesce);
        }
        log.warn("[task.kick] started " + job.getId() + " / " + task.getTaskID() + " = " + job.getDescription());
        queueJobTaskUpdateEvent(job);
    }

    public void stopTask(String jobUUID, int taskID) throws Exception {
        stopTask(jobUUID, taskID, false, false);
    }

    private void stopTask(String jobUUID, int taskID, boolean force, boolean onlyIfQueued) throws Exception {
        Job job = getJob(jobUUID);
        JobTask task = getTask(jobUUID, taskID);
        if (job != null && task != null) {
            taskQueuesByPriority.setStoppedJob(true); // Terminate the current queue iteration cleanly
            HostState host = getHostState(task.getHostUUID());
            if (force) {
                task.setRebalanceSource(null);
                task.setRebalanceTarget(null);
            }
            if (task.getState() == JobTaskState.QUEUED || task.getState() == JobTaskState.QUEUED_HOST_UNAVAIL) {
                removeFromQueue(task);
                log.warn("[task.stop] stopping queued " + task.getJobKey());
            } else if (task.getState() == JobTaskState.REBALANCE) {
                log.warn("[task.stop] stopping rebalancing " + task.getJobKey() + " with force=" + force);
            } else if (task.getState() == JobTaskState.MIGRATING ) {
                log.warn("[task.stop] stopping migrating " + task.getJobKey());
                task.setRebalanceSource(null);
                task.setRebalanceTarget(null);
            }
            else if (force && (task.getState() == JobTaskState.REVERT)) {
                log.warn("[task.stop] " + task.getJobKey() + " killed in revert state");
                int code  = JobTaskErrorCode.EXIT_REVERT_FAILURE;
                job.errorTask(task, code);
                queueJobTaskUpdateEvent(job);
            } else if (force && (host == null || host.isDead() || !host.isUp())) {
                log.warn("[task.stop] " + task.getJobKey() + " killed on down host");
                job.setTaskState(task, JobTaskState.IDLE);
                queueJobTaskUpdateEvent(job);
                return;
            } else if (host != null && !host.hasLive(task.getJobKey())) {
                log.warn("[task.stop] node that minion doesn't think is running: " + task.getJobKey());
                job.setTaskState(task, JobTaskState.IDLE);
                queueJobTaskUpdateEvent(job);
            }
            else if (task.getState() == JobTaskState.ALLOCATED) {
                log.warn("[task.stop] node in allocated state " + jobUUID + "/" + taskID + " host = " + (host != null ? host.getHost() : "unknown"));
            }

            // The following is called regardless of task state, unless the host is nonexistent/failed
            if (host != null) {
                spawnMQ.sendControlMessage(new CommandTaskStop(host.getHostUuid(), jobUUID, taskID, job.getRunCount(), force, onlyIfQueued));
            } else {
                log.warn("[task.stop]" + jobUUID + "/" + taskID + "]: no host monitored for uuid " + task.getHostUUID());
            }
        } else {
            log.warn("[task.stop] job/task {}/{} not found", jobUUID, taskID);
        }
    }

    protected boolean removeFromQueue(JobTask task) {
        boolean removed = false;
        Job job = getJob(task.getJobUUID());
        if (job != null) {
            log.warn("[taskQueuesByPriority] setting " + task.getJobKey() + " as idle and removing from queue");
            job.setTaskState(task, JobTaskState.IDLE);
            removed = taskQueuesByPriority.remove(job.getPriority(), task.getJobKey());
            queueJobTaskUpdateEvent(job);
            sendTaskQueueUpdateEvent();
        }
        writeSpawnQueue();
        return removed;

    }

    public void killTask(String jobUUID, int taskID) throws Exception {
        stopTask(jobUUID, taskID, true, false);
    }

    public void revertJobOrTask(String jobUUID, int taskID, String backupType, int rev, long time) throws Exception {
        if (taskID == -1) {
            // Revert entire job
            Job job = getJob(jobUUID);
            Job.logJobEvent(job, JobEvent.REVERT, eventLog);
            int numTasks = job.getTaskCount();
            for (int i = 0; i < numTasks; i++) {
                log.warn("[task.revert] " + jobUUID + "/" + i);
                revert(jobUUID, backupType, rev, time, i);
            }
        } else {
            // Revert single task
            log.warn("[task.revert] " + jobUUID + "/" + taskID);
            revert(jobUUID, backupType, rev, time, taskID);
        }


    }

    private void revert(String jobUUID, String backupType, int rev, long time, int taskID) throws Exception {
        JobTask task = getTask(jobUUID, taskID);
        if (task != null) {
            task.setPreFailErrorCode(0);
            HostState host = getHostState(task.getHostUUID());
            if (task.getState() == JobTaskState.ALLOCATED || task.getState() == JobTaskState.QUEUED || task.getState() == JobTaskState.QUEUED_HOST_UNAVAIL) {
                log.warn("[task.revert] node in allocated state " + jobUUID + "/" + task.getTaskID() + " host = " + host.getHost());
            }
            log.warn("[task.revert] sending revert message to host: " + host.getHost() + "/" + host.getHostUuid());
            spawnMQ.sendControlMessage(new CommandTaskRevert(host.getHostUuid(), jobUUID, task.getTaskID(), backupType, rev, time, getTaskReplicaTargets(task, task.getAllReplicas()), false));
        } else {
            log.warn("[task.revert] task " + jobUUID + "/" + taskID + "] not found");
        }

    }


    public Collection<String> listCommands() {
        synchronized (spawnState.commands) {
            return spawnState.commands.keySet();
        }
    }

    public JobCommand getCommand(String key) {
        synchronized (spawnState.commands) {
            return spawnState.commands.get(key);
        }
    }

    public void putCommand(String key, JobCommand command, boolean store) throws Exception {
        synchronized (spawnState.commands) {
            spawnState.commands.put(key, command);
        }
        if (useZk && store) {
            spawnDataStore.putAsChild(SPAWN_COMMON_COMMAND_PATH, key, new String(codec.encode(command)));
        }
    }

    public boolean deleteCommand(String key) throws Exception {
        /* prevent deletion of commands used in jobs */
        for (Job job : listJobs()) {
            if (job.getCommand() != null && job.getCommand().equals(key)) {
                return false;
            }
        }
        synchronized (spawnState.commands) {
            JobCommand cmd = spawnState.commands.remove(key);
            if (cmd != null) {
                spawnDataStore.deleteChild(SPAWN_COMMON_COMMAND_PATH, key);
                return true;
            } else {
                return false;
            }
        }
    }

    public Collection<String> listMacros() {
        synchronized (spawnState.macros) {
            return spawnState.macros.keySet();
        }
    }

    public JobMacro getMacro(String key) {
        synchronized (spawnState.macros) {
            return spawnState.macros.get(key.trim());
        }
    }

    public void putMacro(String key, JobMacro macro, boolean store) throws Exception {
        key = key.trim();
        synchronized (spawnState.macros) {
            spawnState.macros.put(key, macro);
        }
        if (store) {
            spawnDataStore.putAsChild(SPAWN_COMMON_MACRO_PATH, key, new String(codec.encode(macro)));
        }
    }

    public boolean deleteMacro(String key) {
        /* prevent deletion of macros used in job configs */
        for (Job job : listJobs()) {
            String rawconf = getJobConfig(job.getId());
            if (rawconf != null && rawconf.contains("%{" + key + "}%")) {
                return false;
            }
        }
        synchronized (spawnState.macros) {
            JobMacro macro = spawnState.macros.remove(key);
            if (macro != null) {
                spawnDataStore.deleteChild(SPAWN_COMMON_MACRO_PATH, key);
                return true;
            } else {
                return false;
            }
        }
    }

    // --------------------- END API ----------------------

    private List<HostState> getOrCreateHostStateList(String minionType, Collection<String> hostList) {
        List<HostState> hostStateList;
        if (hostList == null || hostList.size() == 0) {
            hostStateList = balancer.sortHostsByActiveTasks(listHostStatus(minionType));
        } else {
            hostStateList = new ArrayList<>();
            for (String hostId : hostList) {
                hostStateList.add(getHostState(hostId));
            }
        }
        return hostStateList;
    }


    /**
     * mq message dispatch
     */
    protected void handleMessage(CoreMessage core) {
        Job job;
        JobTask task;
        if (deadMinionMembers.getMemberSet().contains(core.getHostUuid())) {
            log.warn("[mq.core] ignoring message from host: " + core.getHostUuid() + " because it is dead");
            return;
        }
        switch (core.getMessageType()) {
            default:
                log.warn("[mq.core] unhandled type = " + core.getMessageType());
                break;
            case CMD_TASK_NEW:
                // ignore these replication-related messages sent by minions
                break;
            case STATUS_HOST_INFO:
                Set<String> upMinions = minionMembers.getMemberSet();
                HostState state = (HostState) core;
                HostState oldState = getHostState(state.getHostUuid());
                if (oldState == null) {
                    log.warn("[host.status] from unmonitored " + state.getHostUuid() + " = " + state.getHost() + ":" + state.getPort());
                    taskQueuesByPriority.updateHostAvailSlots(state);
                }
                boolean hostEnabled = true;
                synchronized (disabledHosts) {
                    if (disabledHosts.contains(state.getHost()) || disabledHosts.contains(state.getHostUuid())) {
                        hostEnabled = false;
                        state.setDisabled(true);
                    } else {
                        state.setDisabled(false);
                    }
                }
                // Propagate minion state for ui
                if (upMinions.contains(state.getHostUuid()) && hostEnabled) {
                    state.setUp(true);
                }
                state.setUpdated();
                sendHostUpdateEvent(state);
                updateHostState(state);
                break;
            case STATUS_TASK_BEGIN:
                StatusTaskBegin begin = (StatusTaskBegin) core;
                tasksStartedPerHour.mark();
                if (debug("-begin-")) {
                    log.info("[task.begin] :: " + begin.getJobKey());
                }
                try {
                    job = getJob(begin.getJobUuid());
                    if (job == null) {
                        log.warn("[task.begin] on dead job " + begin.getJobKey() + " from " + begin.getHostUuid());
                        break;
                    }
                    if (job.getStartTime() == null) {
                        job.setStartTime(System.currentTimeMillis());
                    }
                    task = job.getTask(begin.getNodeID());
                    if (!checkTaskMessage(task, begin.getHostUuid())) {
                        break;
                    }
                    if (task != null) {
                        job.setTaskState(task, JobTaskState.BUSY);
                        task.incrementStarts();
                        queueJobTaskUpdateEvent(job);
                    } else {
                        log.warn("[task.begin] done report for missing node " + begin.getJobKey());
                    }
                } catch (Exception ex) {
                    log.warn("", ex);
                }
                break;
            case STATUS_TASK_CANT_BEGIN:
                StatusTaskCantBegin cantBegin = (StatusTaskCantBegin) core;
                log.info("[task.cantbegin] received cantbegin from " + cantBegin.getHostUuid() + " for task " + cantBegin.getJobUuid() + "," + cantBegin.getNodeID());
                job = getJob(cantBegin.getJobUuid());
                task = getTask(cantBegin.getJobUuid(), cantBegin.getNodeID());
                if (job != null && task != null) {
                    if (!checkTaskMessage(task, cantBegin.getHostUuid())) {
                        break;
                    }
                    try {
                        job.setTaskState(task, JobTaskState.IDLE);
                        log.info("[task.cantbegin] kicking " + task.getJobKey());
                        startTask(cantBegin.getJobUuid(), cantBegin.getNodeID(), true, true, true);
                    } catch (Exception ex) {
                        log.warn("[task.schedule] failed to reschedule task for " + task.getJobKey(), ex);
                    }
                } else {
                    log.warn("[task.cantbegin] received cantbegin from " + cantBegin.getHostUuid() + " for nonexistent job " + cantBegin.getJobUuid());
                }
                break;
            case STATUS_TASK_PORT:
                StatusTaskPort port = (StatusTaskPort) core;
                job = getJob(port.getJobUuid());
                task = getTask(port.getJobUuid(), port.getNodeID());
                if (task != null) {
                    log.info("[task.port] " + job.getId() + "/" + task.getTaskID() + " @ " + port.getPort());
                    task.setPort(port.getPort());
                    queueJobTaskUpdateEvent(job);
                }
                break;
            case STATUS_TASK_BACKUP:
                StatusTaskBackup backup = (StatusTaskBackup) core;
                job = getJob(backup.getJobUuid());
                task = getTask(backup.getJobUuid(), backup.getNodeID());
                if (task != null && task.getState() != JobTaskState.REBALANCE && task.getState() != JobTaskState.MIGRATING) {
                    log.info("[task.backup] " + job.getId() + "/" + task.getTaskID());
                    job.setTaskState(task, JobTaskState.BACKUP);
                    queueJobTaskUpdateEvent(job);
                }
                break;
            case STATUS_TASK_REPLICATE:
                StatusTaskReplicate replicate = (StatusTaskReplicate) core;
                job = getJob(replicate.getJobUuid());
                task = getTask(replicate.getJobUuid(), replicate.getNodeID());
                if (task != null) {
                    if (!checkTaskMessage(task, replicate.getHostUuid())) {
                        break;
                    }
                    log.info("[task.replicate] " + job.getId() + "/" + task.getTaskID());
                    JobTaskState taskState = task.getState();
                    if (taskState != JobTaskState.REBALANCE && taskState != JobTaskState.MIGRATING) {
                        job.setTaskState(task, replicate.isFullReplication() ? JobTaskState.FULL_REPLICATE : JobTaskState.REPLICATE, true);
                    }
                    queueJobTaskUpdateEvent(job);
                }
                break;
            case STATUS_TASK_REVERT:
                StatusTaskRevert revert = (StatusTaskRevert) core;
                job = getJob(revert.getJobUuid());
                task = getTask(revert.getJobUuid(), revert.getNodeID());
                if (task != null) {
                    if (!checkTaskMessage(task, revert.getHostUuid())) {
                        break;
                    }
                    log.info("[task.revert] " + job.getId() + "/" + task.getTaskID());
                    job.setTaskState(task, JobTaskState.REVERT, true);
                    queueJobTaskUpdateEvent(job);
                }
                break;
            case STATUS_TASK_REPLICA:
                StatusTaskReplica replica = (StatusTaskReplica) core;
                job = getJob(replica.getJobUuid());
                task = getTask(replica.getJobUuid(), replica.getNodeID());
                if (task != null) {
                    for (JobTaskReplica taskReplica : task.getReplicas()) {
                        if (taskReplica.getHostUUID().equals(replica.getHostUuid())) {
                            taskReplica.setVersion(replica.getVersion());
                            taskReplica.setLastUpdate(replica.getUpdateTime());
                        }
                    }
                    log.info("[task.replica] version updated for " + job.getId() + "/" + task.getTaskID() + " ver " + task.getRunCount() + "/" + replica.getVersion());
                    queueJobTaskUpdateEvent(job);
                }
                break;
            case STATUS_TASK_END:
                StatusTaskEnd end = (StatusTaskEnd) core;
                log.info("[task.end] :: " + end.getJobUuid() + "/" + end.getNodeID() + " exit=" + end.getExitCode());
                tasksCompletedPerHour.mark();
                try {
                    job = getJob(end.getJobUuid());
                    if (job == null) {
                        log.warn("[task.end] on dead job " + end.getJobKey() + " from " + end.getHostUuid());
                        break;
                    }
                    task = job.getTask(end.getNodeID());
                    if (!checkTaskMessage(task, end.getHostUuid())) {
                        break;
                    }
                    if (task.isRunning()) {
                        taskQueuesByPriority.incrementHostAvailableSlots(end.getHostUuid());
                    }
                    handleStatusTaskEnd(job, task, end);
                } catch (Exception ex) {
                    log.warn("Failed to handle end message: " + ex, ex);
                }
                break;
        }
    }

    /**
     * Before updating task state, make sure the message source matches the host of the task
     * @param task The task to consider
     * @param messageSourceUuid The source of the message regarding that task
     * @return True if the message source matches the task's expected host
     */
    private boolean checkTaskMessage(JobTask task, String messageSourceUuid) {
        if (task == null || messageSourceUuid == null || !messageSourceUuid.equals(task.getHostUUID())) {
            log.warn("Ignoring task state message from non-live host {}", messageSourceUuid);
            nonHostTaskMessageCounter.inc();
            return false;
        }
        return true;
    }

    /**
     * Handle the various actions in response to a StatusTaskEnd sent by a minion
     *
     * @param job    The job to modify
     * @param task   The task to modify
     * @param update The message
     */
    private void handleStatusTaskEnd(Job job, JobTask task, StatusTaskEnd update) {
        TaskExitState exitState = update.getExitState();
        boolean more = exitState != null && exitState.hadMoreData();
        boolean wasStopped = exitState != null && exitState.getWasStopped();
        task.setFileCount(update.getFileCount());
        task.setByteCount(update.getByteCount());
        boolean errored = update.getExitCode() != 0 && update.getExitCode() != JobTaskErrorCode.REBALANCE_PAUSE;
        if (update.getRebalanceSource() != null) {
            handleRebalanceFinish(job, task, update);
        } else {
            if (exitState != null) {
                task.setInput(exitState.getInput());
                task.setMeanRate(exitState.getMeanRate());
                task.setTotalEmitted(exitState.getTotalEmitted());
            }
            if (more) {
                job.setHadMoreData(more);
            }
            task.setWasStopped(wasStopped);
        }
        if (errored) {
            handleTaskError(job, task, update.getExitCode());
        } else if (!update.wasQueued()) {
            job.setTaskFinished(task);
        }
        if (job.isFinished() && update.getRebalanceSource() == null) {
            finishJob(job, errored);
        }
        queueJobTaskUpdateEvent(job);
    }

    public void handleTaskError(Job job, JobTask task, int exitCode) {
        log.warn("[task.end] " + task.getJobKey() + " exited abnormally with " + exitCode);
        task.incrementErrors();
        try {
            spawnJobFixer.fixTask(job, task, exitCode);
        } catch (Exception ex) {
            job.errorTask(task, exitCode);
        }
    }

    public void handleRebalanceFinish(Job job, JobTask task, StatusTaskEnd update) {
        String rebalanceSource = update.getRebalanceSource();
        String rebalanceTarget = update.getRebalanceTarget();
        if (update.getExitCode() == 0) {
            // Rsync succeeded. Swap to the new host, assuming it is still healthy.
            task.setRebalanceSource(null);
            task.setRebalanceTarget(null);
            if (checkHostStatesForSwap(task.getJobKey(), rebalanceSource, rebalanceTarget, false)) {
                if (task.getHostUUID().equals(rebalanceSource)) {
                    task.setHostUUID(rebalanceTarget);
                } else {
                    task.replaceReplica(rebalanceSource, update.getRebalanceTarget());
                }

                deleteTask(job.getId(), rebalanceSource, task.getTaskID(), false);
                if (update.wasQueued()) {
                    addToTaskQueue(task.getJobKey(), false, false);
                }
            } else {
                // The hosts returned by end message were not found, or weren't in a usable state.
                fixTaskDir(job.getId(), task.getTaskID(), true, true);
            }
        } else if (update.getExitCode() == JobTaskErrorCode.REBALANCE_PAUSE) {
            // Rebalancing was paused. No special action necessary.
            log.warn("[task.move] task rebalance for " + task.getJobKey() + " paused until next run");
        } else {
            // The rsync failed. Clean up the extra task directory.
            fixTaskDir(job.getId(), task.getTaskID(), true, true);
        }
    }

    private static void emailNotification(String jobId, String state, Exception e) {
        if (backgroundEmailAddress != null) {
            long currentTime = System.currentTimeMillis();
            long lastTime = emailLastFired.get();
            if (((currentTime - lastTime) / MILLISECONDS_PER_MINUTE) > backgroundEmailMinute) {
                String subject = "Background operation failed -" + clusterName + "- {" + jobId + "}";
                Writer stringWriter = new StringWriter();
                PrintWriter printWriter = new PrintWriter(stringWriter);
                printWriter.append("The operation for job \"");
                printWriter.append(jobId);
                printWriter.append("\" in state ");
                printWriter.append(state);
                printWriter.append(" throw an exception ");
                e.printStackTrace(printWriter);
                if (e.getCause() != null) {
                    printWriter.append(" caused by ");
                    e.getCause().printStackTrace(printWriter);
                }
                printWriter.flush();
                EmailUtil.email(backgroundEmailAddress, subject, stringWriter.toString());
                emailLastFired.set(currentTime);
            }
        }
    }

    private void doOnState(Job job, String url, int timeout, String state) {
        if (Strings.isEmpty(url)) {
            return;
        }
        if (url.startsWith("http://")) {
            try {
                quietBackgroundPost(job.getId(), state, url, timeout, codec.encode(job));
            } catch (Exception e) {
                log.error("", e);
                emailNotification(job.getId(), state, e);
            }
        } else if (url.startsWith("kick://")) {
            Map<String, List<String>> aliasMap = getAliases();
            for (String kick : Strings.splitArray(url.substring(7), ",")) {
                kick = kick.trim();
                List<String> aliases = aliasMap.get(kick);
                if (aliases != null) {
                    for (String alias : aliases) {
                        safeStartJob(alias.trim());
                    }
                } else {
                    safeStartJob(kick);
                }
            }
        } else {
            log.warn("invalid onState url: " + url + " for " + job.getId());
        }
    }

    private void safeStartJob(String uuid) {
        try {
            startJob(uuid, false);
        } catch (Exception ex) {
            log.warn("[safe.start] " + uuid + " failed due to " + ex);
        }
    }

    /**
     * Perform cleanup tasks once per job completion. Triggered when the last running task transitions to an idle state.
     * In particular: perform any onComplete/onError triggers, set the end time, and possibly do a fixdirs.
     * @param job     The job that just finished
     * @param errored Whether the job ended up in error state
     */
    private void finishJob(Job job, boolean errored) {
        log.info("[job.done] " + job.getId() + " :: errored=" + errored + ". callback=" + job.getOnCompleteURL());
        jobsCompletedPerHour.mark();
        job.setFinishTime(System.currentTimeMillis());
        spawnFormattedLogger.finishJob(job);
        if (!quiesce) {
            if (job.isEnabled() && !errored) {
                /* rekick if any task had more work to do */
                if (job.hadMoreData()) {
                    log.warn("[job.done] " + job.getId() + " :: rekicking on more data");
                    try {
                        scheduleJob(job, false);
                    } catch (Exception ex) {
                        log.warn("", ex);
                    }
                } else {
                    doOnState(job, job.getOnCompleteURL(), job.getOnCompleteTimeout(), "onComplete");
                    if (ENABLE_JOB_FIXDIRS_ONCOMPLETE && job.getRunCount() > 1) {
                        // Perform a fixDirs on completion, cleaning up missing replicas/orphans.
                        fixTaskDir(job.getId(), -1, false, true);
                    }
                }
            } else {
                doOnState(job, job.getOnErrorURL(), job.getOnErrorTimeout(), "onError");
            }
        }
        Job.logJobEvent(job, JobEvent.FINISH, eventLog);
        balancer.requestJobSizeUpdate(job.getId(), 0);
    }


    private static class BackgroundPost implements Runnable {

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
                int postTimeout = timeout > 0 ? (timeout * 1000) : backgroundHttpTimeout;
                HttpUtil.httpPost(url, "javascript/text", post, postTimeout);
            } catch (IOException ex) {
                log.error("IOException when attempting to contact \"" +
                          url + "\" in background task \"" + jobId + " " + state + "\"", ex);
                Throwable cause = ex.getCause();
                if (cause != null) {
                    log.error("Caused by ", cause);
                }
                emailNotification(jobId, state, ex);
            }
        }
    }

    private void quietBackgroundPost(String jobId, String state, String url, int timeout, byte[] post) {
        BackgroundPost task = new BackgroundPost(jobId, state, url, timeout, post);
        try {
            backgroundService.submit(task);
        } catch (RejectedExecutionException ex) {
            log.error("Unable to submit task \"" + jobId + " " + state + "\" for execution", ex);
            emailNotification(jobId, state, ex);
        }
    }

    /**
     * debug output, can be disabled by policy
     */
    private boolean debug(String match) {
        return debug != null && (debug.contains(match) || debug.contains("-all-"));
    }

    @VisibleForTesting
    JobMacro createJobHostMacro(String job, int port) {
        String sPort = Integer.valueOf(port).toString();
        Set<String> jobHosts = new TreeSet<>();// best set?
        jobLock.lock();
        try {
            Collection<HostState> hosts = listHostStatus(null);
            Map<String, String> uuid2Host = new HashMap<>();
            for (HostState host : hosts) {
                if (host.isUp()) {
                    uuid2Host.put(host.getHostUuid(), host.getHost());
                }
            }
            if (uuid2Host.size() == 0) {
                log.warn("[createJobHostMacro] warning job was found on no available hosts: " + job);
            }
            IJob ijob = getJob(job);
            if (ijob == null) {
                log.warn("[createJobHostMacro] Unable to get job config for job: " + job);
                throw new RuntimeException("[createJobHostMacro] Unable to get job config for job: " + job);
            }
            for (JobTask task : ijob.getCopyOfTasks()) {
                String host = uuid2Host.get(task.getHostUUID());
                if (host != null) {
                    jobHosts.add(host);
                }
            }
        } finally {
            jobLock.unlock();
        }

        List<String> hostStrings = new ArrayList<>();
        for (String host : jobHosts) {
            hostStrings.add("{host:\"" + host + "\", port:" + sPort + "}");
        }
        return new JobMacro("spawn", "createJobHostMacro-" + job, Joiner.on(',').join(hostStrings));
    }

    // TODO: 1. Why is this not in SpawnMQ?  2.  Who actually listens to job config changes
    // TODO: answer: this is for the web ui and live updating via SpawnManager /listen.batch

    /**
     * send job update event to registered listeners (usually http clients)
     */
    private void sendJobUpdateEvent(Job job) {
        jobLock.lock();
        try {
            if (jobConfigManager != null) {
                jobConfigManager.updateJob(job);
            }
        } finally {
            jobLock.unlock();
        }
        sendJobUpdateEvent("job.update", job);
    }

    public void queueJobTaskUpdateEvent(Job job) {
        jobLock.lock();
        try {
            jobUpdateQueue.add(job.getId());
        } finally {
            jobLock.unlock();
        }
    }

    private void drainJobTaskUpdateQueue() {
        long start = System.currentTimeMillis();
        Set<String> jobIds = new HashSet<>();
        jobUpdateQueue.drainTo(jobIds);
        if (jobIds.size() > 0) {
            if (log.isTraceEnabled()) {
                log.trace("[drain] Draining " + jobIds.size() + " jobs from the update queue");
            }
            for (String jobId : jobIds) {
                Job job = getJob(jobId);
                sendJobUpdateEvent(job);
            }
            if (log.isTraceEnabled()) {
                log.trace("[drain] Finished Draining " + jobIds.size() + " jobs from the update queue in " + (System.currentTimeMillis() - start) + "ms");
            }
        }
    }

    private synchronized void jobTaskUpdateHeartbeatCheck() {
        try {
            String now = Long.toString(System.currentTimeMillis());
            spawnDataStore.put(SpawnDataStoreKeys.SPAWN_JOB_CONFIG_HEARTBEAT_PATH, now);
            String received = spawnDataStore.get(SpawnDataStoreKeys.SPAWN_JOB_CONFIG_HEARTBEAT_PATH);
            if (received != null && received.equals(now)) {
                jobTaskUpdateHeartbeatSuccessMeter.mark();
            } else {
                jobTaskUpdateHeartbeatFailureCounter.inc();
            }
        } catch (Exception e) {
            jobTaskUpdateHeartbeatFailureCounter.inc();
            log.warn("Failed to perform jobtaskupdate heartbeat check", e);
        }

    }

    public void sendJobUpdateEvent(String label, Job job) {
        try {
            sendEventToClientListeners(label, getJobUpdateEvent(job));
        } catch (Exception e) {
            log.warn("", e);
        }
    }

    /**
     * This method adds a cluster.quiesce event to  be sent to clientListeners to notify those using the UI that the cluster
     * has been quiesced.
     *
     * @param username
     */
    public void sendClusterQuiesceEvent(String username) {
        try {
            boolean quiesce = getSettings().getQuiesced();
            JSONObject info = new JSONObject();
            info.put("username", username);
            info.put("date", JitterClock.globalTime());
            info.put("quiesced", quiesce);
            log.info("User " + username + " has " + (quiesce ? "quiesced" : "unquiesed") + " the cluster.");
            sendEventToClientListeners("cluster.quiesce", info);
        } catch (Exception e) {
            log.warn("", e);
        }
    }

    /**
     * Adds the task.queue.size event to be sent to clientListeners on next batch.listen update
     */
    public void sendTaskQueueUpdateEvent() {
        try {
            int numQueued = 0;
            int numQueuedWaitingOnError = 0;
            LinkedList<JobKey>[] queues = null;
            taskQueuesByPriority.lock();
            try {
                //noinspection unchecked
                queues = taskQueuesByPriority.values().toArray(new LinkedList[taskQueuesByPriority.size()]);

                for (LinkedList<JobKey> queue : queues) {
                    numQueued += queue.size();
                    for (JobKey key : queue) {
                        Job job = getJob(key);
                        if (job != null && !job.isEnabled()) {
                            numQueuedWaitingOnError += 1;
                        }
                    }
                }
                lastQueueSize = numQueued;
            } finally {
                taskQueuesByPriority.unlock();
            }
            JSONObject json = new JSONObject("{'size':" + Integer.toString(numQueued) + ",'sizeErr':" + Integer.toString(numQueuedWaitingOnError) + "}");
            sendEventToClientListeners("task.queue.size", json);
        } catch (Exception e) {
            log.warn("[task.queue.update] received exception while sending task queue update event (this is ok unless it happens repeatedly) " + e);
            e.printStackTrace();
        }
    }

    public int getLastQueueSize() {
        return lastQueueSize;
    }

    public JSONObject getJobUpdateEvent(IJob job) throws Exception {
        long files = 0;
        long bytes = 0;
        int running = 0;
        int errored = 0;
        int done = 0;
        if (job == null) {
            String errMessage = "getJobUpdateEvent called with null job";
            log.warn(errMessage);
            throw new Exception(errMessage);
        }
        List<JobTask> jobNodes = job.getCopyOfTasks();
        int numNodes = 0;
        if (jobNodes != null) {
            numNodes = jobNodes.size();
            for (JobTask task : jobNodes) {
                files += task.getFileCount();
                bytes += task.getByteCount();
                if (task.getState() != JobTaskState.ALLOCATED && task.getState() != JobTaskState.QUEUED && task.getState() != JobTaskState.QUEUED_HOST_UNAVAIL) {
                    running++;
                }
                switch (task.getState()) {
                    case IDLE:
                        done++;
                        break;
                    case ERROR:
                        done++;
                        errored++;
                        break;
                }
            }
        }
        JSONObject ojob = job.toJSON().put("config", "").put("parameters", "");
        ojob.put("nodes", numNodes);
        ojob.put("running", running);
        ojob.put("errored", errored);
        ojob.put("done", done);
        ojob.put("files", files);
        ojob.put("bytes", bytes);
        return ojob;
    }

    protected void sendHostUpdateEvent(HostState state) {
        sendHostUpdateEvent("host.update", state);
    }

    private void sendHostUpdateEvent(String label, HostState state) {
        try {
            sendEventToClientListeners(label, getHostStateUpdateEvent(state));
        } catch (Exception e) {
            log.warn("", e);
        }
    }

    public JSONObject getHostStateUpdateEvent(HostState state) throws Exception {
        if (state == null) {
            return null;
        }
        JSONObject ohost = CodecJSON.encodeJSON(state);
        ohost.put("spawnState", getSpawnStateString(state));
        ohost.put("stopped", ohost.getJSONArray("stopped").length());
        ohost.put("total", state.countTotalLive());
        double score = 0;
        try {
            score = balancer.getHostScoreCached(state.getHostUuid());
        } catch (NullPointerException npe) {
            log.warn("[host.status] exception in getHostStateUpdateEvent", npe);
        }
        ohost.put("score", score);
        return ohost;
    }

    private String getSpawnStateString(HostState state) {
        if (state.isDead()) {
            return "failed";
        } else if (state.isDisabled()) {
            return "disabled";
        }
        return hostFailWorker.getFailureStateString(state.getHostUuid(), state.isUp());
    }

    /**
     * send codable message to registered listeners as json
     */
    private void sendEventToClientListeners(final String topic, final JSONObject message) {
        long time = System.currentTimeMillis();
        for (Entry<String, ClientEventListener> ev : listeners.entrySet()) {
            ClientEventListener client = ev.getValue();
            boolean queueTooLarge = clientDropQueueSize > 0 && client.events.size() > clientDropQueueSize;
            // Drop listeners we haven't heard from in a while, or if they don't seem to be consuming from their queue
            if (time - client.lastSeen > clientDropTimeMillis || queueTooLarge) {
                ClientEventListener listener = listeners.remove(ev.getKey());
                if (debug("-listen-")) {
                    log.warn("[listen] dropping listener queue for " + ev.getKey() + " = " + listener);
                }
                if (queueTooLarge) {
                    nonConsumingClientDropCounter.inc();
                }
                continue;
            }
            try {
                client.events.put(new ClientEvent(topic, message));
            } catch (Exception ex) {
                log.warn("", ex);
            }
        }
        webSocketManager.addEvent(new ClientEvent(topic, message));
    }

    private class UpdateEventRunnable implements Runnable {

        private final Map<String, Long> events = new HashMap<>();

        @Override
        public void run() {
            HostCapacity hostmax = new HostCapacity();
            HostCapacity hostused = new HostCapacity();
            synchronized (monitored) {
                for (HostState hs : monitored.values()) {
                    hostmax.add(hs.getMax());
                    hostused.add(hs.getUsed());
                }
            }
            int jobshung = 0;
            int jobrunning = 0;
            int jobscheduled = 0;
            int joberrored = 0;
            int taskallocated = 0;
            int taskbusy = 0;
            int taskerrored = 0;
            int taskqueued = 0;
            long files = 0;
            long bytes = 0;
            jobLock.lock();
            try {
                for (Job job : spawnState.jobs.values()) {
                    for (JobTask jn : job.getCopyOfTasks()) {
                        switch (jn.getState()) {
                            case ALLOCATED:
                                taskallocated++;
                                break;
                            case BUSY:
                                taskbusy++;
                                break;
                            case ERROR:
                                taskerrored++;
                                break;
                            case IDLE:
                                break;
                            case QUEUED:
                                taskqueued++;
                                break;
                            case QUEUED_HOST_UNAVAIL:
                                taskqueued++;
                                break;
                        }
                        files += jn.getFileCount();
                        bytes += jn.getByteCount();
                    }
                    switch (job.getState()) {
                        case IDLE:
                            break;
                        case RUNNING:
                            jobrunning++;
                            if (job.getStartTime() != null && job.getMaxRunTime() != null &&
                                (JitterClock.globalTime() - job.getStartTime() > job.getMaxRunTime() * 2)) {
                                jobshung++;
                            }
                            break;
                        case SCHEDULED:
                            jobscheduled++;
                            break;
                    }
                    if (job.getState() == JobState.ERROR) {
                        joberrored++;
                    }
                }
            } finally {
                jobLock.unlock();
            }
            events.clear();
            events.put("time", System.currentTimeMillis());
            events.put("hosts", (long) monitored.size());
            events.put("commands", (long) spawnState.commands.size());
            events.put("macros", (long) spawnState.macros.size());
            events.put("jobs", (long) spawnState.jobs.size());
            events.put("cpus", (long) hostmax.getCpu());
            events.put("cpus_used", (long) hostused.getCpu());
            events.put("mem", (long) hostmax.getMem());
            events.put("mem_used", (long) hostused.getMem());
            events.put("io", (long) hostmax.getIo());
            events.put("io_used", (long) hostused.getIo());
            events.put("jobs_running", (long) jobrunning);
            events.put("jobs_scheduled", (long) jobscheduled);
            events.put("jobs_errored", (long) joberrored);
            events.put("jobs_hung", (long) jobshung);
            events.put("tasks_busy", (long) taskbusy);
            events.put("tasks_allocated", (long) taskallocated);
            events.put("tasks_queued", (long) taskqueued);
            events.put("tasks_errored", (long) taskerrored);
            events.put("files", files);
            events.put("bytes", bytes);
            spawnFormattedLogger.periodicState(events);
            runningTaskCount.set(taskbusy);
            queuedTaskCount.set(taskqueued);
            failTaskCount.set(taskerrored);
            runningJobCount.set(jobrunning);
            queuedJobCount.set(jobscheduled);
            failJobCount.set(joberrored);
            hungJobCount.set(jobshung);
        }
    }

    private void require(boolean test, String msg) throws Exception {
        if (!test) {
            throw new Exception("test failed with '" + msg + "'");
        }
    }

    /**
     * called by Thread registered to Runtime triggered by sig-kill
     */
    void runtimeShutdownHook() {
        shuttingDown.set(true);
        try {
            drainJobTaskUpdateQueue();
        } catch (Exception ex) {
            log.warn("", ex);
        }

        try {
            if (spawnFormattedLogger != null) {
                spawnFormattedLogger.close();
            }
        } catch (Exception ex) {
            log.warn("", ex);
        }

        try {
            minionMembers.shutdown();
            deadMinionMembers.shutdown();
        } catch (IOException e) {
            log.warn("unable to cleanly shutdown membership listeners", e);
        }

        try {
            closeZkClients();
        } catch (Exception ex) {
            log.warn("", ex);
        }
    }

    /**
     * re-kicks jobs which are on a repeating schedule
     */
    private class JobRekickTask implements Runnable {

        public void run() {
            boolean kicked;

            do {
                kicked = false;
                /*
                 * cycle through jobs and look for those that need nodes
                 * allocated. lock to prevent other RPCs from conflicting with scheduling.
                 */
                try {
                    if (!quiesce) {
                        String jobids[] = null;
                        jobLock.lock();
                        try {
                            jobids = new String[spawnState.jobs.size()];
                            jobids = spawnState.jobs.keySet().toArray(jobids);
                        } finally {
                            jobLock.unlock();
                        }
                        long clock = System.currentTimeMillis();
                        for (String jobid : jobids) {
                            Job job = getJob(jobid);
                            if (job == null) {
                                log.warn("ERROR: missing job for id " + jobid);
                                continue;
                            }
                            if (job.getState() == JobState.IDLE && job.getStartTime() == null && job.getEndTime() == null) {
                                job.setEndTime(clock);
                            }
                            // check for recurring jobs (that aren't already running)
                            if (job.shouldAutoRekick(clock)) {
                                try {
                                    if (scheduleJob(job, false)) {
                                        log.info("[schedule] rekicked " + job.getId());
                                        kicked = true;
                                    }
                                } catch (Exception ex) {
                                    log.warn("[schedule] ex while rekicking, disabling " + job.getId());
                                    job.setEnabled(false);
                                    updateJob(job);
                                    throw new Exception(ex);
                                }
                            }
                        }
                    }
                } catch (Exception ex) {
                    log.warn("auto rekick failed: ", ex);
                }
            }
            while (kicked);
        }
    }

    protected void autobalance() {
        autobalance(SpawnBalancer.RebalanceType.HOST, SpawnBalancer.RebalanceWeight.HEAVY);
    }

    protected void autobalance(SpawnBalancer.RebalanceType type, SpawnBalancer.RebalanceWeight weight) {
        executeReallocationAssignments(balancer.getAssignmentsForAutoBalance(type, weight), false);
    }

    private boolean schedulePrep(Job job) {
        job.setSubmitCommand(getCommand(job.getCommand()));
        if (job.getSubmitCommand() == null) {
            log.warn("[schedule] failed submit : invalid command " + job.getCommand());
            return false;
        }
        return job.isEnabled();
    }

    private ReplicaTarget[] getTaskReplicaTargets(JobTask task, List<JobTaskReplica> replicaList) {
        ReplicaTarget replicas[] = null;
        if (replicaList != null) {
            int next = 0;
            replicas = new ReplicaTarget[replicaList.size()];
            for (JobTaskReplica replica : replicaList) {
                HostState host = getHostState(replica.getHostUUID());
                if (host == null) {
                    log.warn("[getTaskReplicaTargets] error - replica host: " + replica.getHostUUID() + " does not exist!");
                    throw new RuntimeException("[getTaskReplicaTargets] error - replica host: " + replica.getHostUUID() + " does not exist.  Rebalance the job to correct issue");
                }
                replicas[next++] = new ReplicaTarget(host.getHostUuid(), host.getHost(), host.getUser(), host.getPath(), task.getReplicationFactor());
            }
        }
        return replicas;
    }

    /**
     * Attempt to kick a task. Add it to the queue instead if appropriate.
     *
     * @param job           Job to kick
     * @param task          Task to kick
     * @param config        Config for the job
     * @param inQueue       Whether the task is already in the queue (in which case we shouldn't add it again)
     * @param ignoreQuiesce Whether the task can kick regardless of Spawn's quiesce state
     * @throws Exception If there is a problem scheduling the task
     */
    private void kickIncludingQueue(Job job, JobTask task, String config, boolean inQueue, boolean ignoreQuiesce) throws Exception {
        boolean success = false;
        while (!success && !shuttingDown.get()) {
            jobLock.lock();
            try {
                if (taskQueuesByPriority.tryLock()) {
                    success = true;
                    boolean kicked = kickOnExistingHosts(job, task, config, 0L, true);
                    if (!kicked && !inQueue) {
                        addToTaskQueue(task.getJobKey(), ignoreQuiesce, false);
                    }
                }
            } finally {
                jobLock.unlock();
                if (success) {
                    taskQueuesByPriority.unlock();
                }
            }
        }
    }

    /**
     * Schedule every task from a job.
     *
     * @param job          Job to kick
     * @param isManualKick If the kick is coming from the UI, which is specially allowed to run during quiesce
     * @return True if the job is scheduled successfully
     * @throws Exception If there is a problem scheduling a task
     */
    private boolean scheduleJob(Job job, boolean isManualKick) throws Exception {
        if (!schedulePrep(job)) {
            return false;
        }
        job.setSubmitTime(JitterClock.globalTime());
        job.setStartTime(null);
        job.setEndTime(null);
        job.setHadMoreData(false);
        job.incrementRunCount();
        Job.logJobEvent(job, JobEvent.SCHEDULED, eventLog);
        log.info("[job.schedule] assigning " + job.getId() + " with " + job.getCopyOfTasks().size() + " tasks");
        jobsStartedPerHour.mark();
        for (JobTask task : job.getCopyOfTasks()) {
            if (task == null || task.getState() != JobTaskState.IDLE) {
                continue;
            }
            addToTaskQueue(task.getJobKey(), isManualKick && quiesce, false);
        }
        updateJob(job);
        return true;
    }

    /* helper for SpawnMesh */
    CommandTaskKick getCommandTaskKick(Job job, JobTask task) {
        JobCommand jobCmd = job.getSubmitCommand();
        final String expandedJob;
        try {
            expandedJob = expandJob(job);
        } catch (TokenReplacerOverflowException e) {
            return null;
        }
        CommandTaskKick kick = new CommandTaskKick(
                task.getHostUUID(),
                task.getJobKey(),
                job.getPriority(),
                job.getCopyOfTasks().size(),
                job.getMaxRunTime() != null ? job.getMaxRunTime() * 60000 : 0,
                job.getRunCount(),
                expandedJob,
                Strings.join(jobCmd.getCommand(), " "),
                Strings.isEmpty(job.getKillSignal()) ? null : job.getKillSignal(),
                job.getHourlyBackups(),
                job.getDailyBackups(),
                job.getWeeklyBackups(),
                job.getMonthlyBackups(),
                getTaskReplicaTargets(task, task.getAllReplicas())
                );
        kick.setRetries(job.getRetries());
        return kick;
    }

    public class ScheduledTaskKick implements Runnable {

        public String jobId;
        public Collection<JobParameter> jobParameters;
        public String jobConfig;
        public String rawJobConfig;
        public SpawnMQ spawnMQ;
        public CommandTaskKick kick;
        public Job job;
        public JobTask task;

        public ScheduledTaskKick(String jobId, Collection<JobParameter> jobParameters, String jobConfig, String rawJobConfig, SpawnMQ spawnMQ, CommandTaskKick kick, Job job, JobTask task) {
            this.jobId = jobId;
            this.jobParameters = jobParameters;
            this.jobConfig = jobConfig;
            this.rawJobConfig = rawJobConfig;
            this.spawnMQ = spawnMQ;
            this.kick = kick;
            this.job = job;
            this.task = task;
        }

        public void run() {
            try {
                if (jobConfig == null) {
                    jobConfig = expandJob(jobId, jobParameters, rawJobConfig);
                }
                kick.setConfig(jobConfig);
                spawnMQ.sendJobMessage(kick);
                if (debug("-task-")) {
                    log.info("[task.schedule] assigned " + jobId + "[" + kick.getNodeID() + "/" + (kick.getJobNodes() - 1) + "] to " + kick.getHostUuid());
                }
            } catch (Exception e) {
                log.warn("failed to kick job " + jobId + " task " + kick.getNodeID() + " on host " + kick.getHostUuid() + ":\n" + e);
                jobLock.lock();
                try {
                    job.errorTask(task, JobTaskErrorCode.KICK_ERROR);
                } finally {
                    jobLock.unlock();
                }
            }
        }
    }

    /**
     * Send a start message to a minion.
     *
     * @param job    Job to kick
     * @param task   Task to kick
     * @param config Config for job
     * @return True if the start message is sent successfully
     */
    public boolean scheduleTask(Job job, JobTask task, String config) {
        if (!schedulePrep(job)) {
            return false;
        }
        if (task.getState() != JobTaskState.IDLE && task.getState() != JobTaskState.ERROR && task.getState() != JobTaskState.QUEUED) {
            return false;
        }
        JobState oldState = job.getState();
        if (!job.setTaskState(task, JobTaskState.ALLOCATED)) {
            return false;
        }
        if (oldState == JobState.IDLE && job.getRunCount() <= task.getRunCount()) {
            job.incrementRunCount();
            job.setEndTime(null);
        }
        task.setRunCount(job.getRunCount());
        task.setErrorCode(0);
        task.setPreFailErrorCode(0);
        JobCommand jobcmd = job.getSubmitCommand();

        if (task.getRebalanceSource() != null && task.getRebalanceTarget() != null) {
            // If a rebalance was stopped cleanly, resume it.
            if (new TaskMover(this, task.getJobKey(), task.getRebalanceTarget(), task.getRebalanceSource()).execute()) {
                return true;
            } else {
                // Unable to complete the stopped rebalance. Clear out source/target and kick as normal.
                task.setRebalanceSource(null);
                task.setRebalanceTarget(null);
            }
        }
        CommandTaskKick kick = new CommandTaskKick(
                task.getHostUUID(),
                task.getJobKey(),
                job.getPriority(),
                job.getCopyOfTasks().size(),
                job.getMaxRunTime() != null ? job.getMaxRunTime() * 60000 : 0,
                job.getRunCount(),
                null,
                Strings.join(jobcmd.getCommand(), " "),
                Strings.isEmpty(job.getKillSignal()) ? null : job.getKillSignal(),
                job.getHourlyBackups(),
                job.getDailyBackups(),
                job.getWeeklyBackups(),
                job.getMonthlyBackups(),
                getTaskReplicaTargets(task, task.getAllReplicas())
        );
        kick.setRetries(job.getRetries());

        // Creating a runnable to expand the job and send kick message outside of the main queue-iteration thread.
        // Reason: the jobLock is held for duration of the queue-iteration and expanding some (kafka) jobs can be very
        // slow.  By making job expansion non-blocking we prevent other (UI) threads from waiting on zookeeper.
        // Note: we make a copy of job id, parameters to ignore modifications from outside the queue-iteration thread
        ArrayList<JobParameter> jobParameters = new ArrayList<>();          // deep clone of JobParameter list
        for (JobParameter parameter : job.getParameters()) {
            jobParameters.add(new JobParameter(parameter.getName(), parameter.getValue(), parameter.getDefaultValue()));
        }
        ScheduledTaskKick scheduledKick = new ScheduledTaskKick(job.getId(), jobParameters, config, getJobConfig(job.getId()), spawnMQ, kick, job, task);
        expandKickExecutor.submit(scheduledKick);
        return true;
    }

    /**
     * Helper function for kickOnExistingHosts.
     *
     * @param task A task, typically one that is about to be kicked
     * @return a List of HostStates from the task, either live or replica,
     * that are unable to support a task kick (down, read-only, or scheduled to be failed)
     */
    private List<HostState> hostsBlockingTaskKick(JobTask task) {
        List<HostState> unavailable = new ArrayList<>();
        HostState liveHost = getHostState(task.getHostUUID());
        if (shouldBlockTaskKick(liveHost)) {
            unavailable.add(liveHost);
        }
        List<JobTaskReplica> replicas = (task.getReplicas() != null ? task.getReplicas() : new ArrayList<JobTaskReplica>());
        for (JobTaskReplica replica : replicas) {
            HostState replicaHost = getHostState(replica.getHostUUID());
            if (shouldBlockTaskKick(replicaHost)) {
                unavailable.add(replicaHost);
            }
        }
        return unavailable;
    }

    private boolean shouldBlockTaskKick(HostState host) {
        if (host == null || !host.canMirrorTasks() || host.isReadOnly()) {
            return true;
        }
        HostFailWorker.FailState failState = hostFailWorker.getFailureState(host.getHostUuid());
        return failState == HostFailWorker.FailState.DISK_FULL || failState == HostFailWorker.FailState.FAILING_FS_DEAD;
    }

    /**
     * Attempt to find a host that has the capacity to run a task. Try the live host first, then any replica hosts,
     * swapping onto them only if one is available and if allowed to do so.
     *
     * @param job           Job to kick
     * @param task          Task to kick
     * @param config        Config for job
     * @param timeOnQueue   Time that the task has been on the queue
     * @param allowSwap     Whether to allow swapping to replica hosts
     * @return True if some host had the capacity to run the task and the task was sent there; false otherwise
     */
    public boolean kickOnExistingHosts(Job job, JobTask task, String config, long timeOnQueue, boolean allowSwap) {
        if (!jobTaskCanKick(job, task)) {
            return false;
        }
        List<HostState> possibleHosts = new ArrayList<>();
        if (allowSwap && isNewTask(task)) {
            possibleHosts.addAll(listHostStatus(job.getMinionType()));
        }
        else {
            possibleHosts.addAll(getHealthyHostStatesHousingTask(task, allowSwap));
        }
        HostState bestHost = findHostWithAvailableSlot(task, timeOnQueue, possibleHosts, false);
        if (bestHost != null) {
            String bestHostUuid = bestHost.getHostUuid();
            if (task.getHostUUID().equals(bestHostUuid)) {
                taskQueuesByPriority.markHostTaskActive(bestHostUuid);
                scheduleTask(job, task, config);
                log.info("[taskQueuesByPriority] sending " + task.getJobKey() + " to " + bestHostUuid);
                return true;
            } else if (swapTask(task, bestHostUuid, true)) {
                taskQueuesByPriority.markHostTaskActive(bestHostUuid);
                log.info("[taskQueuesByPriority] swapping " + task.getJobKey() + " onto " + bestHostUuid);
                return true;
            }
        } else if (taskQueuesByPriority.isMigrationEnabled() && !job.getQueryConfig().getCanQuery() && !job.getDontAutoBalanceMe()) {
            return attemptMigrateTask(job, task, timeOnQueue);
        }
        return false;
    }

    private boolean jobTaskCanKick(Job job, JobTask task) {
        if (job == null || !job.isEnabled()) {
            return false;
        }
        boolean isNewTask = isNewTask(task);
        List<HostState> unavailableHosts = hostsBlockingTaskKick(task);
        if (isNewTask && !unavailableHosts.isEmpty()) {
            // If a task is new, just replace any down hosts since there is no existing data
            boolean changed = replaceDownHosts(task);
            if (changed) {
                return false; // Reconsider the task the next time through the queue
            }
        }
        if (!unavailableHosts.isEmpty()) {
            log.warn("[taskQueuesByPriority] cannot kick " + task.getJobKey() + " because one or more of its hosts is down or scheduled to be failed: " + unavailableHosts.toString());
            if (task.getState() != JobTaskState.QUEUED_HOST_UNAVAIL) {
                job.setTaskState(task, JobTaskState.QUEUED_HOST_UNAVAIL);
                queueJobTaskUpdateEvent(job);
            }
            return false;
        } else if (task.getState() == JobTaskState.QUEUED_HOST_UNAVAIL) {
            // Task was previously waiting on an unavailable host, but that host is back. Update state accordingly.
            job.setTaskState(task, JobTaskState.QUEUED);
            queueJobTaskUpdateEvent(job);
        }
        // Obey the maximum simultaneous task running limit for this job, if it is set.
        return !(job.getMaxSimulRunning() > 0 && job.getCountActiveTasks() >= job.getMaxSimulRunning());
    }

    private List<HostState> getHealthyHostStatesHousingTask(JobTask task, boolean allowReplicas) {
        List<HostState> rv = new ArrayList<>();
        HostState liveHost = getHostState(task.getHostUUID());
        if (liveHost != null && hostFailWorker.shouldKickTasks(task.getHostUUID())) {
            rv.add(liveHost);
        }
        if (allowReplicas && task.getReplicas() != null) {
            for (JobTaskReplica replica : task.getReplicas()) {
                HostState replicaHost = replica.getHostUUID() != null ? getHostState(replica.getHostUUID()) : null;
                if (replicaHost != null && replicaHost.hasLive(task.getJobKey()) && hostFailWorker.shouldKickTasks(task.getHostUUID())) {
                    rv.add(replicaHost);
                }
            }
        }
        return rv;
    }

    /**
     * Select a host that can run a task
     *
     * @param task         The task being moved
     * @param timeOnQueue  How long the task has been on the queue
     * @param hosts        A collection of hosts
     * @param forMigration Whether the host in question is being used for migration
     * @return A suitable host that has an available task slot, if one exists; otherwise, null
     */
    private HostState findHostWithAvailableSlot(JobTask task, long timeOnQueue, List<HostState> hosts, boolean forMigration) {
        if (hosts == null) {
            return null;
        }
        List<HostState> filteredHosts = new ArrayList<>();
        for (HostState host : hosts) {
            if (host == null || (forMigration && hostFailWorker.getFailureState(host.getHostUuid()) != HostFailWorker.FailState.ALIVE)) {
                // Don't migrate onto hosts that are being failed in any capacity
                continue;
            }
            if (forMigration && !taskQueuesByPriority.shouldMigrateTaskToHost(task, host.getHostUuid())) {
                // Not a valid migration target
                continue;
            }
            if (isNewTask(task) && !taskQueuesByPriority.shouldKickNewTaskOnHost(timeOnQueue, host)) {
                // Not a valid target for new tasks
                continue;
            }
            if (host.canMirrorTasks() && !host.isReadOnly() && taskQueuesByPriority.shouldKickTaskOnHost(host.getHostUuid())) {
                filteredHosts.add(host);
            }
        }
        return taskQueuesByPriority.findBestHostToRunTask(filteredHosts, true);
    }

    /**
     * Consider migrating a task to a new host and run it there, subject to a limit on the overall number of such migrations
     * to do per time interval and how many bytes are allowed to be migrated.
     *
     * @param job         The job for the task to kick
     * @param task        The task to kick
     * @param timeOnQueue How long the task has been on the queue
     * @return True if the task was migrated
     */
    private boolean attemptMigrateTask(Job job, JobTask task, long timeOnQueue) {
        HostState target;
        if (
                !quiesce &&  // If spawn is not quiesced,
                taskQueuesByPriority.checkSizeAgeForMigration(task.getByteCount(), timeOnQueue) &&
                // and the task is small enough that migration is sensible
                (target = findHostWithAvailableSlot(task, timeOnQueue, listHostStatus(job.getMinionType()), true)) != null)
        // and there is a host with available capacity that can run the job,
        {
            // Migrate the task to the target host and kick it on completion
            log.warn("Migrating " + task.getJobKey() + " to " + target.getHostUuid());
            taskQueuesByPriority.markMigrationBetweenHosts(task.getHostUUID(), target.getHostUuid());
            taskQueuesByPriority.markHostTaskActive(target.getHostUuid());
            TaskMover tm = new TaskMover(this, task.getJobKey(), target.getHostUuid(), task.getHostUUID());
            tm.setMigration(true);
            tm.execute();
            return true;
        }
        return false;
    }

    protected boolean isNewTask(JobTask task) {
        HostState liveHost = getHostState(task.getHostUUID());
        return liveHost != null && !liveHost.hasLive(task.getJobKey()) && task.getFileCount() == 0 && task.getByteCount() == 0;
    }

    /**
     * Add a jobkey to the appropriate task queue, given its priority
     *
     * @param jobKey        The jobkey to add
     * @param ignoreQuiesce Whether the task can kick regardless of Spawn's quiesce state
     */
    public void addToTaskQueue(JobKey jobKey, boolean ignoreQuiesce, boolean toHead) {
        Job job = getJob(jobKey.getJobUuid());
        JobTask task = getTask(jobKey.getJobUuid(), jobKey.getNodeNumber());
        if (job != null && task != null) {
            if (task.getState() == JobTaskState.QUEUED || job.setTaskState(task, JobTaskState.QUEUED)) {
                log.info("[taskQueuesByPriority] adding " + jobKey + " to queue with ignoreQuiesce=" + ignoreQuiesce);
                taskQueuesByPriority.addTaskToQueue(job.getPriority(), jobKey, ignoreQuiesce, toHead);
                queueJobTaskUpdateEvent(job);
                sendTaskQueueUpdateEvent();
            } else {
                log.warn("[task.queue] failed to add task " + jobKey + " with state " + task.getState());
            }
        }
    }

    /**
     * Iterate over each queue looking for jobs that can run. By design, the queues are processed in descending order
     * of priority, so we try priority 2 tasks before priority 1, etc.
     */
    public void kickJobsOnQueue() {
        LinkedList[] queues = null;
        boolean success = false;
        while (!success && !shuttingDown.get()) {
            // need the job lock first
            jobLock.lock();
            try {
                if (taskQueuesByPriority.tryLock()) {
                    success = true;
                    taskQueuesByPriority.setStoppedJob(false);
                    taskQueuesByPriority.updateAllHostAvailSlots(listHostStatus(null));
                    queues = taskQueuesByPriority.values().toArray(new LinkedList[taskQueuesByPriority.size()]);
                    for (LinkedList<SpawnQueueItem> queue : queues) {
                        iterateThroughTaskQueue(queue);
                    }
                    sendTaskQueueUpdateEvent();
                }
            } finally {
                jobLock.unlock();
                if (success) {
                    taskQueuesByPriority.unlock();
                }
            }
            if (!success) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    /**
     * Iterate over a particular queue of same-priority tasks, kicking any that can run.
     * Must be inside of a block synchronized on the queue.
     *
     * @param queue The queue to look over
     */
    private void iterateThroughTaskQueue(LinkedList<SpawnQueueItem> queue) {
        ListIterator<SpawnQueueItem> iter = queue.listIterator(0);
        int skippedQuiesceCount = 0;
        long now = System.currentTimeMillis();
        while (iter.hasNext() && !taskQueuesByPriority.getStoppedJob()) // Terminate if out of tasks or we stopped a job, requiring a queue modification
        {
            SpawnQueueItem key = iter.next();
            Job job = getJob(key.getJobUuid());
            JobTask task = getTask(key.getJobUuid(), key.getNodeNumber());
            try {
                boolean kicked;
                if (job == null || task == null || (task.getState() != JobTaskState.QUEUED && task.getState() != JobTaskState.QUEUED_HOST_UNAVAIL)) {
                    log.warn("[task.queue] removing invalid task " + key);
                    iter.remove();
                    continue;
                }
                if (quiesce && !key.getIgnoreQuiesce()) {
                    skippedQuiesceCount++;
                    if (log.isDebugEnabled()) {
                        log.debug("[task.queue] skipping " + key + " because spawn is quiesced and the kick wasn't manual");
                    }
                    continue;
                } else {
                    kicked = kickOnExistingHosts(job, task, null, now - key.getCreationTime(), !job.getDontAutoBalanceMe());
                }
                if (kicked) {
                    log.info("[task.queue] removing kicked task " + task.getJobKey());
                    iter.remove();
                }
            } catch (Exception ex) {
                log.warn("[task.queue] received exception during task kick: ", ex);
                if (task != null && job != null) {
                    job.errorTask(task, JobTaskErrorCode.KICK_ERROR);
                    iter.remove();
                    queueJobTaskUpdateEvent(job);
                }
            }
        }
        if (skippedQuiesceCount > 0) {
            log.warn("[task.queue] skipped " + skippedQuiesceCount + " queued tasks because spawn is quiesced and the kick wasn't manual");
        }
    }

    /**
     * browser polling event listener
     */
    public static class ClientEventListener {

        public long lastSeen;
        public LinkedBlockingQueue<ClientEvent> events = new LinkedBlockingQueue<ClientEvent>();
    }

    /**
     * event queued to a browser ClientListener
     */
    public static final class ClientEvent implements Codec.Codable {

        private String topic;
        private JSONObject message;

        public ClientEvent(String topic, JSONObject message) {
            this.topic = topic;
            this.message = message;
        }

        public String topic() {
            return topic;
        }

        public JSONObject message() {
            return message;
        }

        public JSONObject toJSON() throws Exception {
            return new JSONObject().put("topic", topic).put("message", message);
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof ClientEvent) {
                ClientEvent ce = (ClientEvent) o;
                return ce.topic == topic && ce.message == message;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return message.hashCode();
        }
    }

    public WebSocketManager getWebSocketManager() {
        return this.webSocketManager;
    }

    public boolean areAlertsEnabled() {
        String alertsEnabled = null;
        try {
            alertsEnabled = spawnDataStore.get(SPAWN_COMMON_ALERT_PATH);
        } catch (Exception ex) {
            log.warn("Unable to read alerts status due to : " + ex.getMessage());
        }
        return alertsEnabled == null || alertsEnabled.equals("") || alertsEnabled.equals("true");
    }

    public void disableAlerts() throws Exception {
        spawnDataStore.put(SPAWN_COMMON_ALERT_PATH, "false");
        this.jobAlertRunner.disableAlerts();
    }

    public void enableAlerts() throws Exception {
        spawnDataStore.put(SPAWN_COMMON_ALERT_PATH, "true");
        this.jobAlertRunner.enableAlerts();
    }

    public List<String> getJobsToAutobalance() {
        List<String> rv = new ArrayList<String>();
        List<Job> autobalanceJobs = balancer.getJobsToAutobalance(listHostStatus(null));
        if (autobalanceJobs == null) {
            return rv;
        }
        for (Job job : autobalanceJobs) {
            if (job.getId() != null) {
                rv.add(job.getId());
            }
        }
        return rv;
    }

    public long getTaskTrueSize(String jobId, int node) {
        return balancer.getTaskTrueSize(getTask(jobId, node));
    }

    public void toggleHosts(String hosts, boolean disable) {
        if (hosts != null) {
            String[] hostsArray = hosts.split(",");
            for (String host : hostsArray) {
                if (host.isEmpty()) {
                    continue;
                }
                boolean changed;
                synchronized (disabledHosts) {
                    changed = disable ? disabledHosts.add(host) : disabledHosts.remove(host);
                }
                if (changed) {
                    updateToggledHosts(host, disable);
                }
            }
            writeState();
        }
    }

    public void updateToggledHosts(String id, boolean disable) {
        for (HostState host : listHostStatus(null)) {
            if (id.equals(host.getHost()) || id.equals(host.getHostUuid())) {
                host.setDisabled(disable);
                sendHostUpdateEvent(host);
                updateHostState(host);
            }
        }
    }

    /**
     * simple settings wrapper allows changes to Spawn
     */
    public class Settings {

        public String getDebug() {
            return debug;
        }

        public void setDebug(String debug) {
            Spawn.this.debug = debug;
            writeState();
        }

        public int getDefaultReplicaCount() {
            return defaultReplicaCount;
        }

        public String getQueryHost() {
            return queryHost;
        }

        public String getSpawnHost() {
            return spawnHost;
        }

        public void setQueryHost(String queryHost) {
            Spawn.this.queryHost = queryHost;
            writeState();
        }

        public void setSpawnHost(String spawnHost) {
            Spawn.this.spawnHost = spawnHost;
            writeState();
        }

        public boolean getQuiesced() {
            return quiesce;
        }

        public void setQuiesced(boolean quiesced) {
            quiesceCount.clear();
            if (quiesced) {
                quiesceCount.inc();
            }
            Spawn.this.quiesce = quiesced;
            writeState();
        }

        public String getDisabled() {
            synchronized (disabledHosts) {
                return Strings.join(disabledHosts.toArray(), ",");
            }
        }

        public void setDisabled(String disabled) {
            synchronized (disabledHosts) {
                disabledHosts.clear();
                disabledHosts.addAll(Arrays.asList(disabled.split(",")));
            }
        }

        public JSONObject toJSON() throws JSONException {
            return new JSONObject().put("debug", debug).put("quiesce", quiesce).put("queryHost", queryHost).put("spawnHost", spawnHost).put("disabled", getDisabled()).put("defaultReplicaCount", defaultReplicaCount);
        }
    }

    public void updateSpawnBalancerConfig(SpawnBalancerConfig newConfig) {
        spawnState.balancerConfig = newConfig;
        balancer.setConfig(newConfig);
    }

    public void writeSpawnBalancerConfig() {
        try {
            spawnDataStore.put(SPAWN_BALANCE_PARAM_PATH, new String(codec.encode(spawnState.balancerConfig)));
        } catch (Exception e) {
            log.warn("Warning: failed to persist SpawnBalancer parameters: ", e);
        }
    }

    protected final void loadSpawnBalancerConfig() {
        String configString = spawnDataStore.get(SPAWN_BALANCE_PARAM_PATH);
        if (configString != null && !configString.isEmpty()) {
            SpawnBalancerConfig loadedConfig = new SpawnBalancerConfig();
            try {
                codec.decode(loadedConfig, configString.getBytes());
                updateSpawnBalancerConfig(loadedConfig);
            } catch (Exception e) {
                log.warn("Warning: failed to decode SpawnBalancerConfig: ", e);
            }
        }
    }

    public SpawnState getSpawnState() {
        return spawnState;
    }

    public SpawnDataStore getSpawnDataStore() {
        return spawnDataStore;
    }

    @VisibleForTesting
    protected static class SpawnState implements Codec.Codable {

        final ConcurrentMap<String, JobMacro> macros = new ConcurrentHashMapV8<>();
        final ConcurrentMap<String, JobCommand> commands = new ConcurrentHashMapV8<>();
        final ConcurrentMap<String, Job> jobs = new ConcurrentHashMapV8<>();
        final DirectedGraph<String> jobDependencies = new DirectedGraph<>();
        SpawnBalancerConfig balancerConfig = new SpawnBalancerConfig();
    }

}
