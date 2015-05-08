#!/bin/bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# initialization

function testForExecutable() {
    options=$1
    splitOptions=(${options//|/ })
    success=0
    for cmd in "${splitOptions[@]}"
    do
        if type "$cmd" &> /dev/null; then
            success=1
        fi
    done
    if [ "$success" = "0" ]; then
        printOptions=${options//|/\" or \"}
        echo
        echo "ERROR: Cannot find command: \"$printOptions\""
        if [ "${#splitOptions[@]}" -eq "1" ]; then
            echo "Please install the command and try again."
        else
            echo "Please install one of the alternatives and try again."
        fi
        echo
        exit 1
    fi
}

testForExecutable "greadlink|readlink"
testForExecutable "wget"

dirname=$(basename `pwd`)
if [ "${dirname}" != "hydra-local" ]; then
    mkdir -p hydra-local
    cd hydra-local
fi

export PATH=$PATH:/usr/local/sbin
export BIN=$(pwd)/bin


# initialize number of minions using optional argument
number_of_minions=3
if [ $# -ge 2 ] 
then
    number_match=`echo $2 | grep -o "[1-9][0-9]*"`
    if [ $number_match -eq $2 ]
    then
        number_of_minions=$2
    fi
fi
minions=""
for i in `seq 0 1 \`expr $number_of_minions - 1\``
do
    minions=`echo "$minions mini${i}n"`
done


# fetch required jars
(
    for dir in exec zoo log etc streams pid bin cert; do
        [ ! -d $dir ] && mkdir $dir
    done
    [ ! -f bin/job-task.sh ] && cp ../hydra-uber/local/bin/job-task.sh bin/
    [ ! -f cert/keystore.jks ] && cp ../hydra-uber/local/cert/keystore.jks cert/
    [ ! -f cert/keystore.password ] && cp ../hydra-uber/local/cert/keystore.password cert/
    [ ! -h web ] && ln -s ../hydra-main/web web
    (
        cd streams
        echo "setting up stream dir ..."
        rm -f job
        for i in `seq 0 1 \`expr $number_of_minions - 1\``
        do
            if [ ! -h job${i} ]
            then
                ln -s ../mini${i}n/ job${i}
            fi
        done
        [ ! -h log ] && ln -s ../log/ log
    )
    if [ ! -f etc/zookeeper.properties ]; then
        cat > etc/zookeeper.properties << EOF
# the directory where the snapshot is stored.
dataDir=./zoo
# the port at which the clients will connect
clientPort=2181
# the maxmimum number of connections per IP address
maxClientCnxns=1000000
# disable zookeeper jmx log4j to allow non-log4j slf4js
jmx.log4j.disable=true
EOF
    fi
    for minion in $minions; do
        if [ ! -f ${minion}/minion.state ]; then
            mkdir ${minion}
            echo "creating default ${minion} state"
            echo "{stopped:{},uuid:\"${minion}\"}" > ${minion}/minion.state
        fi
    done
)

export HYDRA_CONF=$(pwd)/../hydra-uber

export HYDRA_LOCAL_DIR=$(pwd)/../hydra-local

export HYDRA_EXEC=`ls -t ${HYDRA_CONF}/target/hydra-uber-*exec*jar | head -n 1`

export LOG4J_PROPERTIES="-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager \
-Dlog4j.configurationFactory=com.addthis.hydra.uber.HoconConfigurationFactory -Dlogging.stderr=off"

export MQ_MASTER_OPT="${LOG4J_PROPERTIES} -Xmx1284M -Deps.mem.debug=10000 -Dcs.je.cacheSize=256M \
-Dcs.je.cacheShared=1 -Dcs.je.deferredWrite=1 -Dzk.servers=localhost:2181 -Dstreamserver.read.timeout=60000 \
-Djava.net.preferIPv4Stack=true -Dganglia.enable=false -Dqmaster.mesh.peers=localhost -Dmeshy.senders=1 \
-Dmeshy.stream.prefetch=true -Dqmaster.mesh.peer.port=5101"

export MQ_WORKER_OPT="${LOG4J_PROPERTIES} -Xmx1284M -Dmesh.local.handlers=com.addthis.hydra.data.query.source.MeshQuerySource \
-Dmeshy.stream.prefetch=true -Dmeshy.senders=1"

export MINION_OPT="${LOG4J_PROPERTIES} -Xmx512M -Dminion.mem=512 -Dminion.localhost=localhost -Dminion.group=local \
-Dminion.web.port=0 -Dspawn.localhost=localhost -Dhttp.post.max=327680 -Dminion.sparse.updates=1 \
-Dreplicate.cmd.delay.seconds=1 -Dbackup.cmd.delay.seconds=0 -Dbatch.brokerAddresses=localhost"

export SPAWN_OPT="-Xmx512M ${LOG4J_PROPERTIES} -Dspawn.localhost=localhost -Dspawn.queryhost=localhost -Dspawn.status.interval=6000 \
-Dspawn.chore.interval=3000 -Dhttp.post.max=327680  -Dspawn.polltime=10000 -Dspawnbalance.min.disk.percent.avail.replicas=0.01 \
-Dspawn.auth.ldap=false -Dmesh.port=5000 -Djob.store.remote=false -Dspawn.queue.new.task.last.slot.delay=0 -Dspawn.defaultReplicaCount=0 \
-Dbatch.brokerAddresses=localhost -Dspawn.https.keystore.password=$HYDRA_LOCAL_DIR/cert/keystore.password \
-Dspawn.https.keymanager.password=$HYDRA_LOCAL_DIR/cert/keystore.password \
-Dspawn.https.keystore.path=$HYDRA_LOCAL_DIR/cert/keystore.jks -Dspawn.https.login.default=0"

export MESHY_OPT="-Xmx128M -Xms128M ${LOG4J_PROPERTIES} -Dmeshy.autoMesh=false -Dmeshy.throttleLog=true \
-Dmeshy.buffers.enable=true -Dmeshy.stream.maxopen=10000"

export JAVA_CMD="java -server ${JAVA_OPTS} -Djava.net.preferIPv4Stack=true -Djava.library.path=${ZMQ_LIBDIR} \
-Dhydra.tree.cache.maxSize=250 -Dhydra.tree.page.maxSize=50 -Dcs.je.cacheSize=200M -Deps.mem.debug=3000"

# flcow support for linux
if [ -n "${FLCOW_SO+1}" ]; then
    export LD_PRELOAD=${FLCOW_SO}:${LD_PRELOAD}
    export FLCOW_PATH="^$(pwd)"
    export FLCOW_EXCLUDE="\.((stats)|(pid)|(done)|(complete))$"
fi


if [ ! -f ${HYDRA_EXEC} ]; then
    echo "missing exec jar @ ${HYDRA_EXEC}"
    exit
fi

function startBrokers() {
    quit=0
    if [ ! -f "pid/pid.rabbitmq" ]; then
        touch pid/pid.rabbitmq
        echo "starting rabbitmq"
        nohup rabbitmq-server > log/rabbitmq-server.log 2>&1 &
        quit=1
    fi
    if [ ! -f pid/pid.zookeeper ]; then
        echo "starting zookeeper"
        ${JAVA_CMD} -cp ${HYDRA_EXEC} org.apache.zookeeper.server.quorum.QuorumPeerMain ./etc/zookeeper.properties > log/zookeeper.log 2>&1 &
        echo "$!" > pid/pid.zookeeper
        quit=1
    fi
    if [ $quit -eq 1 ]; then
        echo "------------------------------------------------"
        echo "wait for mq to come up, check status by running rabbitmqctl status"
        echo "(If your OS is running rabbit as a service already, this will have harmlessly failed)"
        echo "then re-run this script"
        exit
    fi
}

function startOthers() {
    if [ ! -f pid/pid.mqworker ]; then
        export HYDRA_LOG=log/worker
        echo "starting mesh query worker"
        ${JAVA_CMD} ${MQ_WORKER_OPT} -jar ${HYDRA_EXEC} mqworker server 5101 ${HYDRA_LOCAL_DIR} localhost:5100 > log/mqworker.log 2>&1 &
        echo "$!" > pid/pid.mqworker
    fi
    if [ ! -f pid/pid.mqmaster ]; then
        export HYDRA_LOG=log/master
        echo "starting mesh query master"
        ${JAVA_CMD} ${MQ_MASTER_OPT} -jar ${HYDRA_EXEC} mqmaster etc web jar > log/mqmaster.log 2>&1 &
        echo "$!" > pid/pid.mqmaster
    fi
    for minion in $minions; do
        if [ ! -f pid/pid.${minion} ]; then
            echo "starting ${minion}"
            jvmname="-Dvisualvm.display.name=${minion}"
            dataDir="-Dcom.addthis.hydra.minion.Minion.dataDir=${minion}"
            echo "${JAVA_CMD} ${MINION_OPT} ${jvmname} ${dataDir} -jar ${HYDRA_EXEC} minion ${minion}" > log/${minion}.cmd
            ${JAVA_CMD} ${MINION_OPT} ${jvmname} ${dataDir} -jar ${HYDRA_EXEC} minion ${minion} > log/${minion}.log 2>&1 &
            echo "$!" > pid/pid.${minion}
        fi
    done
    if [ ! -f pid/pid.spawn ]; then
        echo "starting spawn"
        ${JAVA_CMD} ${SPAWN_OPT} -jar ${HYDRA_EXEC} spawn > log/spawn.log 2>&1 &
        echo "$!" > pid/pid.spawn
    fi
    if [ ! -f pid/pid.meshy ]; then
        echo "starting meshy"
        ${JAVA_CMD} ${MESHY_OPT} -jar ${HYDRA_EXEC} mesh server 5000 streams > log/meshy.log 2>&1 &
        echo "$!" > pid/pid.meshy
    fi
    echo "--------------------------------------------------"
    echo "manage your batch cluster @ http://localhost:5052/"
    if [ ! -f bin/seed.cmd ]; then
        echo "run 'local_stack seed' to install default commands'"
    fi
}

function deletePidFiles() {
    for process in $1; do
        if [ -f pid/pid.${process} ]
        then
            # ps h (--no-header) does not work on mac
            pid=`cat pid/pid.${process}`
            isrunning=`ps $pid | grep $pid | wc -l`
            if [ "$isrunning" -eq "0" ]
            then
                rm pid/pid.${process}
            fi
        fi
    done
}

function silentKill() {
    kill $1 2> /dev/null
    return 0
}

function stopAll() {
    x=0
    deletePidFiles "mqworker mqmaster $minions spawn meshy"
    [ -f pid/pid.mqworker ] && echo "stopping mqworker" && silentKill $(cat pid/pid.mqworker) && x=1
    [ -f pid/pid.mqmaster ] && echo "stopping mqmaster" && silentKill $(cat pid/pid.mqmaster) && x=1
    for minion in $minions; do
        [ -f pid/pid.${minion} ] && echo "stopping ${minion}" && silentKill $(cat pid/pid.${minion}) && x=1
    done
    [ -f pid/pid.spawn ] && echo "stopping spawn" && silentKill $(cat pid/pid.spawn) && x=1
    [ -f pid/pid.meshy ] && echo "stopping meshy" && silentKill $(cat pid/pid.meshy) && x=1
    echo "sleeping for shutdown..."
    sleep 3
    deletePidFiles "mqworker mqmaster $minions spawn meshy"
    if [ $x -eq 0 ]; then
        [ -f pid/pid.zookeeper ] && echo "stopping zookeeper" && kill $(cat pid/pid.zookeeper) && rm pid/pid.zookeeper
        [ -f pid/pid.rabbitmq ] && echo "stopping rabbitmq" && rm pid/pid.rabbitmq && rabbitmqctl stop
    else
        echo "run stop again if you wish to stop brokers (zookeeper and rabbitmq)"
    fi
}

function checkDependencies() {
    ssh localhost echo test >/dev/null 2>&1 || echo >&2 "Warning: your machine does not have ssh access to itself. Job replicas will not work."
    [[ `java -version 2>&1` == 'java version "1.8'* ]] || echo >&2 "Warning: you are using an unsupported java version. Only version 8 is supported."
    if [[ $JAVA_CMD != *-Dqueue.mesh=true* && $SPAWN_OPT != *-Dqueue.mesh=true* ]] 
    then
        type rabbitmq-server >/dev/null 2>&1 || { echo >&2 "Error: Rabbitmq messaging chosen, but the rabbitmq-server utility was not found on your machine. Try installing rabbitmq and making sure it is in your path."; exit 1; }
    fi
    if [[ `uname -a` == *Darwin* ]]
    then
        type gcp >/dev/null 2>&1 || { echo >&2 "Error: Mac OS X detected and gcp not found. You must install coreutils and make sure /usr/local/bin is added to your PATH in .bashrc and .bash_profile."; exit 1; }  
        ssh localhost type gcp >/dev/null 2>&1 || { echo >&2 "Warning: Mac OS X detected and gcp not found when using ssh. Job replicas will not work unless you add /usr/local/bin to your PATH in .bash_profile."; }
    fi
}

case $1 in
    start)
        checkDependencies
        startBrokers
        startOthers
        ;;
    stop)
        stopAll
        ;;
    seed)
        wget -q -O /dev/null 'http://localhost:5050/command.put?label=default-task&owner=install&cpu=1&mem=512&io=1&command=${BIN}/job-task.sh job.conf {{nodes}} {{node}} {{jobid}}'
        ;;
    *)
        echo "commands: start [# minions, default 3] | stop [# minions, default 3] | seed"
        ;;
esac

