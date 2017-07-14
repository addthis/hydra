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
package com.addthis.hydra.job.web;

import com.addthis.basis.kv.KVPairs;

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.auth.InsufficientPrivilegesException;

/**
 * Handles some more complicated job API requests.
 * 
 * This component is intended to support spawn v1 and v2.
 */
public interface JobRequestHandler {

    /** 
     * Creates or updates a job.
     * 
     * @param kv        request parameters
     * @param user      the user who made the request
     * @return          the created/update job
     * @throws IllegalArgumentException If any parameter is invalid (400 response code)
     * @throws InsufficientPrivilegesException If insufficient privileges are available (401 response code)
     * @throws Exception                If any other error occurred, typically an internal one 
     *                                  (500 response code)
     */
    Job createOrUpdateJob(KVPairs kv, String user, String token, String sudo, boolean defaults) throws Exception;

    /**
     * Update the minion type for a job. To update, All tasks need to be currently on hosts with this minion type.
     *
     * @param jobid job id
     * @param minionType new minion type
     * @param user the user who made the request
     * @return the created/update job
     * @throws IllegalArgumentException        If any parameter is invalid (400 response code)
     * @throws InsufficientPrivilegesException If insufficient privileges are available (401 response code)
     * @throws Exception                       If any other error occurred, typically an internal one
     *                                         (500 response code)
     */
    void updateMinionType(Job job, String minionType) throws Exception;

    /**
     * Kicks the specified job if the right parameters are set. (THIS IS A LEGACY METHOD!)
     * 
     * This method supports spawn v1's job.submit end point which is also used for kicking job/task.
     * Do NOT use this method in new code. 
     * 
     * @param kv    request parameters that may contain job/task kicking parameters
     * @param job   the job to kick
     * @return {@code true} if job/task is kicked; {@code false} otherwise
     * @throws Exception if error occurred when kicking the job
     */
    boolean maybeKickJobOrTask(KVPairs kv, Job job) throws Exception;
}
