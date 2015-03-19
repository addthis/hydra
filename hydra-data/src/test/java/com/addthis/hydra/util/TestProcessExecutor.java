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
package com.addthis.hydra.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestProcessExecutor {

    @Test
    public void echoHelloWorld() {
        String[] cmdarray = {"echo", "hello world"};
        ProcessExecutor executor = new ProcessExecutor.Builder(cmdarray).build();
        assertEquals(true, executor.execute());
        assertEquals(0, executor.exitValue());
        assertNotNull(executor.stdout());
        assertNotNull(executor.stderr());
        assertEquals("hello world", executor.stdout().trim());
        assertEquals("", executor.stderr().trim());
    }

    @Test
    public void catStandardInput() {
        String[] cmdarray = {"cat"};
        ProcessExecutor executor = new ProcessExecutor.Builder(cmdarray).setStdin("hello world").build();
        assertEquals(true, executor.execute());
        assertEquals(0, executor.exitValue());
        assertNotNull(executor.stdout());
        assertNotNull(executor.stderr());
        assertEquals("hello world", executor.stdout().trim());
        assertEquals("", executor.stderr().trim());
    }

    @Test
    public void processTimeout() {
        String[] cmdarray = {"sleep", "120"};
        ProcessExecutor executor = new ProcessExecutor.Builder(cmdarray).setWait(1).build();
        assertEquals(false, executor.execute());
    }


}
