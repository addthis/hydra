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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MacUtils {
    private static final Logger log = LoggerFactory.getLogger(MacUtils.class);

    private MacUtils() {}

    static final String cpcmd;
    static final String lncmd;
    static final String lscmd;
    static final String rmcmd;
    static final String mvcmd;
    static final String ducmd;
    static final boolean useMacFriendlyPSCommands;
    static final boolean linkBackup;

    // detect fl-cow in sys env and apple for copy command
    static {
        boolean copyOnWriteSupported = false;
        for (String v : System.getenv().values()) {
            if (v.toLowerCase().contains("libflcow")) {
                log.info("detected support for copy-on-write hard-links");
                copyOnWriteSupported = true;
                break;
            }
        }
        linkBackup = !System.getProperty("minion.backup.hardlink", "0").equals("0") || copyOnWriteSupported;

        boolean useGnuCommands = false;
        for (Object v : System.getProperties().values()) {
            if (v.toString().toLowerCase().contains("apple") || v.toString().toLowerCase().contains("mac os x")) {
                log.info("detected darwin-based system. switching to gnu commands");
                useGnuCommands = true;
                break;
            }
        }
        if (useGnuCommands) {
            cpcmd = "gcp";
            lncmd = "gln";
            lscmd = "gls";
            rmcmd = "grm";
            mvcmd = "gmv";
            ducmd = "gdu";
            useMacFriendlyPSCommands = true;
        } else {
            cpcmd = "cp";
            lncmd = "ln";
            lscmd = "ls";
            rmcmd = "rm";
            mvcmd = "mv";
            ducmd = "du";
            useMacFriendlyPSCommands = false;
        }
    }
}
