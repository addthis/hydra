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

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;

public class TaskRunner {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("error: no arguments specified. Must specify at least one argument.");
            return;
        }
        String fileName = args[0];
        String config = loadStringFromFile(fileName);
        JsonRunner.runTask(config, args);
    }

    static String loadStringFromFile(String fileName) throws IOException {
        return Bytes.toString(Files.read(new File(fileName)));
    }

}
