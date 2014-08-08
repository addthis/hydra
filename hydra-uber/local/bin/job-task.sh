#!/bin/sh
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


if [ -f job.conf ]; then
	extargs=$(grep '// -' job.conf | while read a b; do echo $b; done | tr '\n' ' ' | tr '\r' ' ')
	extjar=$(grep '// jar=' job.conf | tr '=' ' ' | while read a b c; do echo $c; done | tr '\n' ' ' | tr '\r' ' ')
	if [ ! -z "${extjar}" ]; then HYDRA_EXEC=${extjar}; fi
fi

canonical=$(which greadlink || which readlink)
jarpath=$($canonical -m ${HYDRA_EXEC})

eval exec ${JAVA_CMD:-java} \
	-server \
	-Xmx512M \
	-Xms512M \
	-XX:+AggressiveOpts \
	-XX:+UseParallelGC \
	-XX:+UseParallelOldGC \
	-Dlog4j.configurationFactory=com.addthis.hydra.uber.HoconConfigurationFactory \
	-Dorg.jboss.logging.provider=slf4j \
	-Dbatch.brokerHost=${brokerHost:-localhost} \
	-Dbatch.brokerPort=${brokerPort:-5672} \
	-Dbatch.job.log4j=1 \
	-Dcs.je.cacheSize=128M \
	-Dcs.je.cacheShared=1 \
	-Dcs.je.deferredWrite=1 \
	-Dhydra.query.age.max=120000 \
	-Dhydra.query.cache.max=4096 \
	-Dhydra.query.concurrent.timeout=180000 \
	-Dhydra.query.concurrent.max=10 \
	-Dhydra.query.debug=1 \
	-Dhydra.tree.cache.maxSize=0 \
	-Dhydra.tree.cache.maxMem=100M \
	-Dhydra.tree.page.maxSize=200 \
	-Dhydra.tree.page.maxMem=100k \
	-Dhydra.tree.mem.sample=0 \
	-Dhydra.tree.cleanqmax=100 \
	-Dhydra.tree.db=2 \
	-Dhydra.tree.trash.interval=1000 \
	-Dhydra.tree.trash.maxtime=100 \
	-Dmapper.localhost=${host} \
	-DnativeURLCodec=0 \
	-Dje.log.fileMax=100000000 \
	-Dje.cleaner.minUtilization=90 \
	-Dje.cleaner.threads=1 \
	-Dje.evictor.lruOnly=true \
	-Dje.evictor.nodesPerScan=100 \
	-Dstreamserver.read.timeout=10000 \
	-Dtask.exit=1 \
	-Dtask.threads=2 \
	-Dtrak.event.debug=1 \
	-Dzk.servers=${zkservers:-localhost:2181} \
	-Djava.net.preferIPv4Stack=true \
	-Dmapper.tree.type=0 \
	${extargs} \
	-javaagent:${jarpath} \
	-jar ${jarpath} \
	task $*
