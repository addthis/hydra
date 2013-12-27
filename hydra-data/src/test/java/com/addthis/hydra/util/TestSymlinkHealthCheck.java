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

import java.io.File;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestSymlinkHealthCheck {

    @Test
    public void testHealthCheck() {
        File tempDir = null;
        try {
            tempDir = com.addthis.basis.util.Files.createTempDir();
            Path tempDirPath = tempDir.toPath().toAbsolutePath();
            SymlinkHealthCheck check = new SymlinkHealthCheck(tempDirPath);
            assertTrue(check.runCheck());

            java.nio.file.Files.createSymbolicLink(Paths.get(tempDirPath.toString(), "foo"),
                    Paths.get("/dev/null"));
            java.nio.file.Files.createSymbolicLink(Paths.get(tempDirPath.toString(), "bar"),
                    Paths.get("/dev/null"));

            assertFalse(check.runCheck());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (tempDir != null) {
                com.addthis.basis.util.Files.deleteDir(tempDir);
            }
        }
    }

    @Test
    public void testRecursiveDepthCheck() {
        File tempDir = null;
        try {
            tempDir = com.addthis.basis.util.Files.createTempDir();
            Path tempDirPath = tempDir.toPath().toAbsolutePath();
            SymlinkHealthCheck check = new SymlinkHealthCheck(tempDirPath);
            assertTrue(check.runCheck());

            java.nio.file.Files.createSymbolicLink(Paths.get(tempDirPath.toString(), "foo"),
                    Paths.get(tempDirPath.toString(), "foo"));

            assertFalse(check.runCheck());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (tempDir != null) {
                com.addthis.basis.util.Files.deleteDir(tempDir);
            }
        }
    }


}
