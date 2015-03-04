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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.addthis.codec.annotations.Time;

import com.google.common.io.ByteStreams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around Java Process execution API. Does not attempt to
 * consume the output stream or the error stream. Do not use this
 * class if you expect your process to fill those streams. If the
 * stdout or stderr streams are full then the process will block
 * and not complete successfully.
 */
public class ProcessExecutor {

    private static final Logger log = LoggerFactory.getLogger(ProcessExecutor.class);

    @Time(TimeUnit.SECONDS)
    private static final int DEFAULT_WAIT = 10;

    /**
     * Name of command and arguments to execute.
     */
    @Nonnull private final String[] cmdarray;

    /**
     * Optionally specify standard input to process.
     */
    @Nullable private final String stdin;

    /**
     * Number of seconds to wait for process to complete.
     */
    @Time(TimeUnit.SECONDS)
    private final int wait;

    /**
     * Exit code of completed processes. Only valid
     * if {@link #execute()} returns true. Initial value
     * is {@code Integer.MIN_VALUE} to eliminate mistaking
     * an uninitialized value for successful exit.
     */
    private int exitValue = Integer.MIN_VALUE;

    /**
     * Standard output of completed processes. Possibly null.
     */
    private String stdout;

    /**
     * Standard error of completed processes. Possibly null.
     */
    private String stderr;

    private ProcessExecutor(@Nonnull String[] cmdarray, @Nullable String stdin,
                           @Time(TimeUnit.SECONDS) int wait) {
        this.cmdarray = cmdarray;
        this.stdin = stdin;
        this.wait = wait;
    }

    public boolean execute() {
        boolean completed = false;
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(cmdarray);
            if (stdin != null) {
                OutputStreamWriter osw = new OutputStreamWriter(process.getOutputStream());
                osw.write(stdin);
                osw.close();
            }
            completed = process.waitFor(wait, TimeUnit.SECONDS);
            if (!completed) {
                stdout = readAvailable(process.getInputStream());
                stderr = readAvailable(process.getErrorStream());
                process.destroyForcibly();
            } else {
                stdout = new String(ByteStreams.toByteArray(process.getInputStream()));
                stderr = new String(ByteStreams.toByteArray(process.getErrorStream()));
                exitValue = process.exitValue();
            }
            if (!completed) {
                log.error("Process '{}' was killed. It did not complete after {} seconds. " +
                          "Stdout was '{}' and stderr was '{}'", Arrays.toString(cmdarray),
                          wait, stderr, stderr);
            } else if (exitValue != 0) {
                log.error("Process '{}' returned non-zero exit value {}. " +
                          "Stdout was '{}' and stderr was '{}'", Arrays.toString(cmdarray),
                          exitValue, stderr, stderr);
            }
        } catch (IOException|InterruptedException ex) {
            log.error("Exception occurred attempting to execute process '{}'",
                      Arrays.toString(cmdarray), ex);
            if (process != null) {
                process.destroyForcibly();
            }
        }
        return completed;
    }

    private static String readAvailable(InputStream inputStream) throws IOException {
        int available = inputStream.available();
        if (available > 0) {
            byte[] data = new byte[available];
            inputStream.read(data);
            return new String(data);
        } else {
            return null;
        }
    }

    public int exitValue() {
        return exitValue;
    }

    public String stdout() {
        return stdout;
    }

    public String stderr() {
        return stderr;
    }

    public static class Builder {
        // required fields
        @Nonnull private final String[] cmdarray;
        // optional fields
        @Nullable private String stdin;
        private int wait = DEFAULT_WAIT;

        public Builder(@Nonnull String[] cmdarray) {
            this.cmdarray = cmdarray;
        }

        public Builder setStdin(String stdin) {
            this.stdin = stdin;
            return this;
        }

        public Builder setWait(int wait) {
            this.wait = wait;
            return this;
        }


        public ProcessExecutor build() {
            return new ProcessExecutor(cmdarray, stdin, wait);
        }
    }

}
