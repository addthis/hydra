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

import java.io.File;
import java.io.IOException;

import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.LessFiles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class TaskReplacementFile implements TaskStringReplacement {

    private static final Logger log = LoggerFactory.getLogger(TaskReplacementFile.class);

    @Nonnull
    @Override
    public String replace(@Nonnull String input) throws IOException {
        int atpos = input.indexOf(":@file(");
        int cpos = input.indexOf(")", atpos + 7);
        if (atpos >= 0 && cpos > atpos) {
            String path = input.substring(atpos + 7, cpos);
            input = input.replace("@file(" + path + ")", LessBytes.toString(LessFiles.read(new File(path))));
            log.info("found " + path);
        }
        return input;
    }
}
