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
package com.addthis.hydra.minion;

import java.io.File;
import java.io.IOException;

import java.util.Scanner;

import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.LessFiles;
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
                LessFiles.write(tmp, LessBytes.toBytes("#!/bin/sh\n" + cmd + "\n"), false);
                Process process = Runtime.getRuntime().exec("sh " + tmp);
                int exit = waitForUninterruptibly(process);
                log.debug("[shell] ({}) exited with {}", cmd, exit);
                return exit;
            } finally {
                tmp.delete();
            }
        } catch (Exception ex) {
            log.warn("", ex);
        }
        return -1;
    }

    public static int waitForUninterruptibly(Process toWaitFor) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return toWaitFor.waitFor();
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
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
                return Integer.parseInt(LessBytes.toString(LessFiles.read(pidFile)).trim());
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
                cmd = LessBytes.toString(LessFiles.read(cmdFile)).trim().replace('\0', ' ');
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
