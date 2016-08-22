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

import com.addthis.basis.util.LessStrings;
import com.addthis.basis.util.Parameter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * simple util to send emails
 */
public class EmailUtil {

    private static final Logger log = LoggerFactory.getLogger(EmailUtil.class);
    private static final String FROM_ADDRESS = Parameter.value("email.fromAddress");

    public static boolean email(String to, String subject, String body) {
        try {
            final String[] cmd;
            if (FROM_ADDRESS == null) {
                cmd = new String[]{"mailx", "-s " + subject, to};
            } else {
                cmd = new String[]{"mailx", "-r " + FROM_ADDRESS, "-s " + subject, to};
            }
            ProcessExecutor executor = new ProcessExecutor.Builder(cmd).setStdin(body).build();
            boolean success = executor.execute();
            int exitValue = executor.exitValue();
            String standardError = executor.stderr();
            /**
             * If the process completed successfully but the standard error
             * log is non-empty then emit the standard error.
             */
            if (success && (exitValue == 0) && !LessStrings.isEmpty(standardError)) {
                log.warn("Stderr was non-empty in email with subject: {} to : {} due to subshell error : {} {}",
                         subject, to, exitValue, standardError);
            }
            return success && (exitValue == 0);
        } catch (Exception e) {
            log.warn("Unable to send email with subject: {} to : {}", subject, to, e);
            return false;
        }
    }
}
