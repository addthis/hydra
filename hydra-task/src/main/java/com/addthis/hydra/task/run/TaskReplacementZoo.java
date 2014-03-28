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
package com.addthis.hydra.task.run;

import javax.annotation.Nonnull;

import java.io.IOException;

import com.addthis.bark.ZkClientFactory;
import com.addthis.bark.ZkHelpers;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public class TaskReplacementZoo implements TaskRunner.TaskStringReplacement {

    private static final Logger log = LoggerFactory.getLogger(TaskReplacementZoo.class);

    @Nonnull
    @Override
    public String replace(@Nonnull String input) throws IOException {
        int atpos = input.indexOf(":@zoo(");
        int cpos = input.indexOf(")", atpos + 6);
        if (atpos >= 0 && cpos > atpos) {
            String path = input.substring(atpos + 6, cpos);
            input = input.replace("@zoo(" + path + ")", readZoo(path));
            log.warn("found " + path);
        }
        return input;
    }

    public static String readZoo(String path) {
        ZkClient zkClient = ZkClientFactory.makeStandardClient();
        if (ZkHelpers.pathExists(zkClient, path)) {
            return ZkHelpers.readData(zkClient, path);
        }
        throw new RuntimeException("zoo path not found '" + path + "'");
    }
}
