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

import java.io.OutputStreamWriter;

import java.util.concurrent.TimeUnit;

import com.google.common.io.ByteStreams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * simple util to send emails
 */
public class EmailUtil {

    private static final Logger log = LoggerFactory.getLogger(EmailUtil.class);

    public static void email(String to, String subject, String body) {
        try {
            String[] cmd = {"mailx", "-s " + subject, to};
            Process p = Runtime.getRuntime().exec(cmd);
            OutputStreamWriter osw = new OutputStreamWriter(p.getOutputStream());
            osw.write(body);
            osw.close();
            boolean exited = p.waitFor(10, TimeUnit.SECONDS);
            if (exited) {
                String standardError = new String(ByteStreams.toByteArray(p.getErrorStream()));
                int exitCode = p.exitValue();
                if (!standardError.isEmpty() || (exitCode != 0)) {
                    log.warn("Unable to send email with subject: {} to : {} due to subshell error : {} {}",
                             subject, to, exitCode, standardError);
                }
            } else {
                log.warn("Waited 10 seconds for subshell to send email with subject: {} to : {}, but it did not exit.",
                         subject, to);
            }
        } catch (Exception e) {
            log.warn("Unable to send email with subject: {} to : {}", subject, to, e);
        }
    }
}
