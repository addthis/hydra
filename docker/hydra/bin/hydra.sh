#!/bin/bash

source /opt/hydra/bin/bash_profile

function rollLog {
    name=$1
    if [ -f ${HYDRA_LOG}/$name.log ]; then
        current="${HYDRA_LOG}/$name.log"
        old="${HYDRA_LOG}/old/$name-$(date +'%y%m%d%H%M%S').log"
        mkdir -p "${HYDRA_LOG}/old/"
        mv "$current" "$old"
        nohup gzip "$old" >> ${HYDRA_LOG}/old/$name-gzip.log 2>&1 &
    fi
}

function startLoggedJar {
    name=$1
    shift
    arguments=$*
    mkdir -p "${HYDRA_LOG}"
    mkdir -p "${HYDRA_LOG}/gc"
    if [ -f "${HYDRA_PID}/$name.pid" ]; then
        pid=`cat "${HYDRA_PID}/$name.pid"`
        if [ "`ps $pid | grep $pid`" ]; then
            echo "$name is aready running, pid: $pid"
            return 1
        fi
    fi
    rollLog $name
    # eval does expansion on the parameters from `getJvm...`, nohup for consistent shell exit behavior
    # -cp is set to the (also expanded by eval) file path, arguments im unsure on, '\&' to pass it to nohup
    eval nohup java -server `getJvmParameters $name` -cp `getParameter $name jar` $arguments >> ${HYDRA_LOG}/$name.log 2>&1 \&
    pid=$!
    echo $! > "${HYDRA_PID}/$name.pid"
}

function repeatedlyStartJar
{
    name=$1
    if [ -f "${HYDRA_PID}/repeat-$name.pid" ]; then
        pid=`cat "${HYDRA_PID}/repeat-$name.pid"`
        if [ "`ps $pid | grep $pid`" ]; then
            echo "$name is aready running in repeat wrapper, pid: $pid"
            return 1
        fi
    fi
    nohup repeatedlyStartJarScript $* >> "${HYDRA_LOG}/repeat-$name.log" 2>&1 &
    pid=$!
    echo $! > "${HYDRA_PID}/repeat-$name.pid"
}
