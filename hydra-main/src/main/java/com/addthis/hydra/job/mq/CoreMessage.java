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
package com.addthis.hydra.job.mq;

import java.io.Serializable;

import com.addthis.codec.codables.Codable;

public interface CoreMessage extends Codable, Serializable {

    enum TYPE {
        STATUS_HOST_INFO,
        STATUS_TASK_BEGIN,
        STATUS_TASK_PORT,
        STATUS_TASK_END,
        STATUS_TASK_BACKUP,
        STATUS_TASK_REPLICATE,
        STATUS_TASK_REPLICA,
        CMD_TASK_KICK,
        CMD_TASK_STOP,
        CMD_TASK_DELETE,
        CMD_TASK_REVERT,
        CMD_TASK_REPLICATE,
        CMD_TASK_NEW,
        CMD_TASK_PROMOTE_REPLICA,
        STATUS_TASK_JUMP_SHIP,
        STATUS_TASK_REVERT,
        CMD_TASK_DEMOTE_REPLICA,
        STATUS_TASK_CANT_BEGIN,
        CMD_TASK_UPDATE_REPLICAS
    }

    TYPE getMessageType();

    String getHostUuid();
}
