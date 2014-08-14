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

import com.addthis.basis.util.Files;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConfigSubTest {

    @Test
    public void subAt() throws IOException {
        String input = "hello world";
        File tempDir = Files.createTempDir();
        File tempFile = new File(tempDir.getAbsolutePath() + File.separator + "temp");
        Files.write(tempFile, input.getBytes(), true);
        try {
            assertEquals(input, TaskRunner.subAt(input));
            assertEquals(":" + input, TaskRunner.subAt(":@file(" + tempFile.getAbsolutePath() + ")"));
        } finally {
            if (tempDir != null)
                Files.deleteDir(tempDir);
        }
    }

}
