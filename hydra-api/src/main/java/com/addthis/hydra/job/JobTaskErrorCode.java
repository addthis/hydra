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
package com.addthis.hydra.job;

/**
 * A class for defining integer constants for task error codes.
 * Positive error codes are reserved for process exit codes coming from minion StatusTaskEnd events.
 */
public class JobTaskErrorCode {

    public static final int EXIT_BACKUP_FAILURE = -100;
    public static final int EXIT_REPLICATE_FAILURE = -101;
    public static final int EXIT_REVERT_FAILURE = -102;
    public static final int SWAP_FAILURE = -103;
    public static final int HOST_FAILED = -104;
    public static final int KICK_ERROR = -105;
    public static final int EXIT_DIR_ERROR = -106;
    public static final int EXIT_SCRIPT_EXEC_ERROR = -107;
    public static final int REBALANCE_PAUSE = -108;
}


