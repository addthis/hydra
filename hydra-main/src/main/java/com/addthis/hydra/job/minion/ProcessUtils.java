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
package com.addthis.hydra.job.minion;

import java.io.File;
import java.io.IOException;

import java.util.Scanner;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.SimpleExec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ProcessUtils {
    private static final Logger log = LoggerFactory.getLogger(ProcessUtils.class);

    private ProcessUtils() {}

    public static int shell(String cmd, File directory) {
        try {
            File tmp = File.createTempFile(".minion", "shell", directory);
            try {
                Files.write(tmp, Bytes.toBytes("#!/bin/sh\n" + cmd + "\n"), false);
                int exit = Runtime.getRuntime().exec("sh " + tmp).waitFor();
                if (log.isDebugEnabled()) {
                    log.debug("[shell] (" + cmd + ") exited with " + exit);
                }
                return exit;
            } finally {
                tmp.delete();
            }
        } catch (Exception ex) {
            log.warn("", ex);
        }
        return -1;
    }

    static String getTaskBaseDir(String baseDir, String id, int node) {
        return new StringBuilder().append(baseDir).append("/").append(id).append("/").append(node).toString();
    }

    public static Integer getPID(File pidFile) {
        try {
            if (pidFile != null) {
                log.debug("[getpid] {} --> {}", pidFile, pidFile.exists());
            } else {
                log.debug("[getpid] pidFile is null");
            }
            if ((pidFile != null) && pidFile.exists()) {
                return Integer.parseInt(Bytes.toString(Files.read(pidFile)).trim());
            } else {
                return null;
            }
        } catch (Exception ex) {
            log.warn("", ex);
            return null;
        }
    }

    private static String execCommandReturnStdOut(String cmd) throws InterruptedException, IOException {
        SimpleExec command = new SimpleExec(cmd).join();
        return command.stdoutString();
    }

    // Sorry Illumos port...
    public static String getCmdLine(int pid) {
        try {
            String cmd;
            if (MacUtils.useMacFriendlyPSCommands) {
                cmd = execCommandReturnStdOut("ps -f " + pid);
            } else {
                File cmdFile = new File("/proc/" + pid + "/cmdline");
                cmd = Bytes.toString(Files.read(cmdFile)).trim().replace('\0', ' ');
            }
            log.warn("found cmd {}  for pid: {}", cmd, pid);
            return cmd;

        } catch (Exception ex) {
            log.warn("error searching for pidfile for pid: {}", pid, ex);
            return null;
        }
    }

    static boolean activeProcessExistsWithPid(Integer pid, File directory) {
        return shell("ps " + pid, directory) == 0;
    }

    static Integer findActiveRsync(String id, int node) {
        return findActiveProcessWithTokens(new String[]{id + "/" + node + "/", Minion.rsyncCommand}, new String[]{"server"});
    }

    private static Integer findActiveProcessWithTokens(String[] requireTokens, String[] omitTokens) {
        StringBuilder command = new StringBuilder("ps ax | grep -v grep");
        for (String requireToken : requireTokens) {
            command.append("| grep " + requireToken);
        }
        for (String excludeToken : omitTokens) {
            command.append("| grep -v " + excludeToken);
        }
        command.append("| cut -c -5");
        try {
            SimpleExec exec = new SimpleExec(new String[]{"/bin/sh", "-c", command.toString()}).join();
            if (exec.exitCode() == 0) {
                return new Scanner(exec.stdoutString()).nextInt();
            } else {
                return null;
            }
        } catch (Exception e) {
            // No PID found
            return null;
        }

    }
}
