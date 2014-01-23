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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JAR=`ls -1 -t $DIR/../target/*-exec.jar | head -n 1`
echo "[INFO] Executing plugin architecture test using -cp argument."
java -cp $JAR com.addthis.hydra.common.plugins.TestPlugins
STATUS_CP=$?
echo "[INFO] Executing plugin architecture test using -jar argument."
java -jar $JAR testPlugins
STATUS_JAR=$?
if [ $STATUS_CP -ne 0 ] || [ $STATUS_JAR -ne 0 ]; then
  exit 1
fi
