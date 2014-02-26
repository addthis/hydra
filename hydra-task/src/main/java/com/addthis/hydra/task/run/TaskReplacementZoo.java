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

import com.addthis.bark.StringSerializer;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public class TaskReplacementZoo implements TaskRunner.TaskStringReplacement {

    private static final Logger log = LoggerFactory.getLogger(TaskReplacementZoo.class);
    private final CuratorFramework zkClient;

    public TaskReplacementZoo(CuratorFramework zkClient) {
        this.zkClient = zkClient;
    }

    @Nonnull
    @Override
    public String replace(@Nonnull String input) throws IOException {
        int atpos = input.indexOf(":@zoo(");
        int cpos = input.indexOf(")", atpos + 6);
        if (atpos >= 0 && cpos > atpos) {
            String path = input.substring(atpos + 6, cpos);
            try {
                input = input.replace("@zoo(" + path + ")", readZoo(path));
            } catch (Exception e) {
                throw new IOException(e);
            }
            log.warn("found " + path);
        }
        return input;
    }

    public String readZoo(@Nonnull String path) throws Exception {
        if (zkClient.checkExists().forPath(path) != null) {
            return StringSerializer.deserialize(zkClient.getData().forPath(path));
        }
        throw new RuntimeException("zoo path not found '" + path + "'");
    }
}
