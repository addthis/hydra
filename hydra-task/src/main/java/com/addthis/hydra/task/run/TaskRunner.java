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

import java.io.File;
import java.io.IOException;

import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;

public class TaskRunner {

    static final int defaultThreads = Parameter.intValue("task.threads",
            Math.max(1, Runtime.getRuntime().availableProcessors()-1));

    static final Pattern commentPattern = Pattern.compile("\\s*(#|(//))\\s*");

    /**
     * Iterate though all of the lines at the beginning of the file that
     * are identified as comment lines. If the task runner type is located
     * in the comment lines then return the type. When the first non-comment
     * line is encountered then return the default type.
     *
     * @param config job configuration
     * @return job specification type
     */
    public static TaskRunnerType parseTaskType(String config) {

        StringTokenizer tokenizer = new StringTokenizer(config, "\n");
        while (tokenizer.hasMoreTokens()) {
            String line = tokenizer.nextToken();
            Matcher commentMatcher = commentPattern.matcher(line);
            if (commentMatcher.find()) {
                for (TaskRunnerType type : TaskRunnerType.values()) {
                    Matcher typeMatcher = type.getPattern().matcher(line.substring(commentMatcher.end()));
                    if (typeMatcher.find()) {
                        return type;
                    }
                }
            } else {
                return TaskRunnerType.defaultType();
            }
        }
        return TaskRunnerType.defaultType();
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        if (args.length < 1) {
            System.out.println("error: no arguments specified. Must specify at least one argument.");
            return;
        }
        String fileName = args[0];
        String config = loadStringFromFile(fileName);
        TaskRunnerType taskType = parseTaskType(config);
        taskType.runTask(config, args);
    }

    static String loadStringFromFile(String fileName) throws IOException {
        return Bytes.toString(Files.read(new File(fileName)));
    }

}
