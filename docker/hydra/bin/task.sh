#!/bin/bash

source /opt/hydra/bin/bash_profile

echo 7 > /proc/$$/oom_adj

name=$1
shift
arguments="com.addthis.hydra.Main task $*"
jar=`getParameter $name jar`
jarIsAgent=`getParameter $name jar.isAgent 2> /dev/null`

if [ -f job.conf ]; then
  extargs=$(grep '// -' job.conf | while read a b; do echo $b; done | tr '\n' ' ' | tr '\r' ' ')
  extjar=$(grep '// jar=' job.conf | tr '=' ' ' | while read a b c; do echo $c; done | tr '\n' ' ' | tr '\r' ' ')
  if [ ! -z "${extjar}" ]; then jar=${extjar}; fi
fi

jar=`readlink -m $jar`

# prevent javaagents from over writing custom jar classes by supporting
# a last minute optional alias evaluation
agentJar=""
if [ "$jarIsAgent" = "True" ]; then
  agentJar="-javaagent:$jar"
fi

eval exec java -server -D'"plugins.path element.prune-ymd.nameFormat=yyMMdd"' `getJvmParameters $name` "$agentJar" ${extargs} -cp ${jar} $arguments "job-id=${HYDRA_JOBID}" "task-id=${HYDRA_NODE}"