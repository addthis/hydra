#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HOST=localhost
if type boot2docker >/dev/null 2>&1; then
    HOST=192.168.59.103
    BOOT2DOCKER_STATUS=`boot2docker status`
    if [ "$BOOT2DOCKER_STATUS" = "poweroff" ]; then
        boot2docker start
    fi
fi

SCRIPTNAME="${0##*/}"

: ${MESH_PORT:=6500}
: ${QUERY_MESH_PORT:=6600}
: ${QUERY_MESH_PEER_PORT:=6601}
: ${QUERY_WEB_PORT:=6700}
: ${QUERY_API_PORT:=6800}
: ${ZK_SERVERS:=$HOST:2181}
: ${ZK_PORT:=2181}
: ${ZK_CHROOT:="hydra"}
: ${SPAWN_HOST:=$HOST}
: ${QUERY_HOST:=$HOST}
: ${CLUSTER_NAME:=hydra}
: ${SPAWN_WEB_PORT:=5052}
: ${RABBIT_MQ_ADDRESS:=$HOST:5672}
: ${RABBIT_MQ_PORT:=5672}
: ${RABBIT_MQ_ADMIN_PORT:=15672}
: ${HOST:=localhost}
: ${MINION_HOST:=$HOST}
: ${HYDRA_ROOT:="\/opt\/hydra"}

pushd $DIR &> /dev/null
trap "popd &> /dev/null" EXIT

container_build() {
    if [ -z "$2" ];
    then
        VERSION=1.0
    else
        VERSION=$2
    fi
    mkdir -p $HOME/.m2
    sed "s/\${USER}/${USER}/g;s/\${MINION_HOST}/${MINION_HOST}/g;s/\${HYDRA_ROOT}/${HYDRA_ROOT}/g;s/\${RABBIT_MQ_ADDRESS}/${RABBIT_MQ_ADDRESS}/g;s/\${HOST}/${HOST}/g;s/\${CLUSTER_NAME}/${CLUSTER_NAME}/g;s/\${QUERY_HOST}/${QUERY_HOST}/g;s/\${MESH_PORT}/${MESH_PORT}/g;s/\${QUERY_MESH_PORT}/${QUERY_MESH_PORT}/g;s/\${QUERY_MESH_PEER_PORT}/${QUERY_MESH_PEER_PORT}/g;s/\${QUERY_WEB_PORT}/${QUERY_WEB_PORT}/g;s/\${QUERY_API_PORT}/${QUERY_API_PORT}/g;s/\${ZK_SERVERS}/${ZK_SERVERS}/g;s/\${ZK_CHROOT}/${ZK_CHROOT}/g" \
        < ./$1/Dockerfile.template > ./$1/Dockerfile
    echo "Building docker image"
    docker build -t $USER/$1:$VERSION ./$1
}

fullPath () {
  if  [[ $1 == /* ]];
  then
      echo $1
  else
      echo $PWD/$1
  fi
}

hydra_init() {
    docker exec -it $1 wget -q -O /dev/null 'http://localhost:5050/command.put?label=default-task&owner=install&cpu=1&mem=512&io=1&command=nice -n 4 /opt/hydra/bin/task.sh task-generic job.conf {{nodes}} {{node}} {{jobid}}'
}

hydra_createSampleJob() {
    JOBHOST=$1
    if [ -z "$JOBHOST" ];
    then
        JOBHOST=$HOST
    fi
    curl 'http://'${JOBHOST}':5052/job/save'  -H 'Username: hydrauser' --data 'description=samplejob&submitTime=-1&endTime=-1&status=&maxRunTime=&rekickTimeout=&nodes=1&bytes=&command=default-task&stateText=&stateLabel=&minionType=default&autoRetry=false&config=source.empty.maxPackets%3A1000%0A%0Amap.filterOut%3A%5B%0A++++%7Bfrom%3A%22RANDOM%22%2C+filter.random.max%3A10%7D%0A++++%7Bdebug+%7B%7D%7D%0A%5D%0A%0Aoutput.tree+%7B%0A++++root%3A%5B%0A++++++++%7Bconst%3A%22root%22%7D%0A++++++++%7Bfield%3A%22RANDOM%22%7D%0A++++%5D%0A%7D%0A%0A&owner=hydrauser' --compress
}

hydra_runJob() {
    JOBHOST=$2
    if [ -z "$JOBHOST" ];
    then
        JOBHOST=$HOST
    fi
    curl 'http://'$JOBHOST':5052/job/start?jobid='$1  -H 'Username: hydrauser'
}

container_start() {
    if [ -z "$2" ];
    then
        VERSION=1.0
    else
        VERSION=$2
    fi
    FULL_SCRIPTDIR=$(fullPath ./$1)
    if [ -z "$3" ];
    then
        HYDRA_DIR=../
    else
        HYDRA_DIR=$3
    fi
    RABBIT_DIR=$(fullPath $HYDRA_DIR/docker/rabbitmq)
    JAR_DIR=$(fullPath $HYDRA_DIR/hydra-uber/target)
    WEB_DIR=$(fullPath $HYDRA_DIR/hydra-main/web)
    NAME=$1
    case $NAME in
        zookeeper)
            echo "starting zookeeper container"
            mkdir -p zookeeper/conf
            mkdir -p zookeeper/data
            docker run -v $FULL_SCRIPTDIR/data:/opt/zookeeper/data \
                    -p $ZK_PORT:$ZK_PORT \
                    --name $NAME \
                    -d $USER/$NAME:$VERSION
            # create chroot for zookeeper
            docker exec -it $NAME /bin/sh -c "echo create /$ZK_CHROOT \'\' |/opt/zookeeper/bin/zkCli.sh"
            ;;
        hydra)
            echo "starting spawn container $FULL_SCRIPTDIR"
            docker run -v $FULL_SCRIPTDIR/etc:/opt/hydra/etc \
                    -v $FULL_SCRIPTDIR/logs:/var/log/hydra \
                    -v $FULL_SCRIPTDIR/data:/opt/hydra/minion \
                    -v $JAR_DIR:/opt/hydra/jar \
                    -v $WEB_DIR:/opt/hydra/web \
                    -p $SPAWN_WEB_PORT:$SPAWN_WEB_PORT \
                    --name $NAME -d $USER/$NAME:$VERSION
            ;;
        minion)
            echo "starting minion container $FULL_SCRIPTDIR"
            docker run -v $FULL_SCRIPTDIR/etc:/opt/hydra/etc \
                    -v $FULL_SCRIPTDIR/logs:/var/log/hydra \
                    -v $FULL_SCRIPTDIR/data:/opt/hydra/minion \
                    -v $JAR_DIR:/opt/hydra/jar \
                    -v $WEB_DIR:/opt/hydra/web \
                    --name $NAME -d $USER/$NAME:$VERSION
            ;;
        rabbitmq)
            echo "starting rabbitmq container"
            docker run -p $RABBIT_MQ_PORT:$RABBIT_MQ_PORT \
                       -p $RABBIT_MQ_ADMIN_PORT:$RABBIT_MQ_ADMIN_PORT \
                       --name $NAME -d $USER/$NAME:$VERSION
            ;;
        *)
            docker_run_help
            exit 3
            ;;
    esac
}


container_stop() {
    docker stop $1
    docker rm $1
}

container_status() {
    docker ps
}

container_login() {
    docker exec -it $1 bash
}

docker_run_help() {
    echo "" >&2
    echo "Container name must be one of:" >&2
    echo "" >&2
    echo "hydra:      a container running the hydra processes" >&2
    echo "zookeeper: a container running a zookeeper process" >&2
    echo "rabbitmq: a container running a zookeeper process" >&2
}

docker_help() {
    echo "" >&2
    echo "Usage: \$SCRIPTNAME {build|start|stop|login|status|init|help}" >&2
    echo "" >&2
    echo "build:  create or recreate the container" >&2
    echo "buildall:  create or recreate all hydra related containers" >&2
    echo "start:  start container with alias at_site." >&2
    echo "startall:  start all hydra containers." >&2
    echo "stop:   stop and remove container with provided alias" >&2
    echo "stopall:   stop and remove all hydra related containers" >&2
    echo "login:  login into container" >&2
    echo "init:  Initialize the Hydra environment, must be run after Hydra container is up" >&2
    echo "createSampleJob: Create a sample Hydra job" >&2
    echo "runJob: run a job with the provided ID" >&2
    echo "status: show status of all containers" >&2
    echo "help:   show this message" >&2
}

case "$1" in
    build)
        echo "Generating docker container for $NAME"
        container_build $2 $3
        ;;
    buildall)
        echo "Building Hydra, RabbitMQ, and Zookeeper Containers"
        container_build rabbitmq $2
        container_build zookeeper $2
        container_build hydra $2
        ;;
    startall)
        echo "Starting Hydra, RabbitMQ and Zookeeper Containers"
        container_start rabbitmq $2
        container_start zookeeper $2
        container_start hydra $2
        ;;
    stopall)
        echo "Stopping Hydra, RabbitMQ and Zookeeper Containers"
        container_stop hydra
        container_stop rabbitmq
        container_stop zookeeper
        ;;
    start)
        echo "Starting docker container"
        container_start $2 $3 $4
        ;;
    stop)
        echo "Stopping docker container"
        container_stop $2
        ;;
    login)
        echo "ssh into container"
        container_login $2
        ;;
    init)
        echo "initializing hydra"
        hydra_init $2
        ;;
    createSampleJob)
        echo "creating sample job"
        hydra_createSampleJob $2
        ;;
    runJob)
        echo "running job $3"
        hydra_runJob $2 $3
        ;;
    status)
        container_status
        ;;
    *)
        docker_help
        exit 3
        ;;
esac

exit 0
