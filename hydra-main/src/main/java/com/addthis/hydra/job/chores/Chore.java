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
package com.addthis.hydra.job.chores;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A chore that can be submitted to a ChoreWatcher and hopefully finishes
 * at a later time.
 */
public class Chore {

    private long startTime;
    private final long maxRunTimeMillis;
    private ArrayList<ChoreCondition> conditions;
    private ArrayList<ChoreAction> onFinish;
    private ArrayList<ChoreAction> onError;
    private final String key;
    private ChoreStatus status;
    private final Lock lock = new ReentrantLock();

    @JsonCreator
    public Chore(@JsonProperty("key") String key, @JsonProperty("conditions") ArrayList<ChoreCondition> conditions, @JsonProperty("onFinish") ArrayList<ChoreAction> onFinish,
            @JsonProperty("onError") ArrayList<ChoreAction> onError, @JsonProperty("maxRunTimeMillis") long maxRunTimeMillis) {
        this.key = key;
        this.startTime = System.currentTimeMillis();
        this.maxRunTimeMillis = maxRunTimeMillis;
        this.status = ChoreStatus.SUBMITTED;
        this.conditions = conditions;
        this.onFinish = onFinish;
        this.onError = onError;
    }

    public ArrayList<ChoreCondition> getConditions() {
        if (conditions == null) {
            return new ArrayList<ChoreCondition>();
        } else {
            return conditions;
        }
    }

    public String getKey() {
        return key;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getMaxRunTimeMillis() {
        return maxRunTimeMillis;
    }

    public ChoreStatus getStatus() {
        return status;
    }

    public ArrayList<ChoreAction> getOnFinish() {
        return onFinish;
    }

    public ArrayList<ChoreAction> getOnError() {
        return onError;
    }

    public void doFinish(ExecutorService choreExecutor) {
        lock();
        try {
            for (ChoreAction action : onFinish) {
                choreExecutor.submit(action);
            }
        } finally {
            unlock();
        }
    }

    public void doError(ExecutorService choreExecutor) {
        lock();
        try {
            for (ChoreAction action : onError) {
                choreExecutor.submit(action);
            }
        } finally {
            unlock();
        }
    }

    public boolean setFinished() {
        lock();
        try {
            if (status != ChoreStatus.SUBMITTED) return false;
            status = ChoreStatus.FINISHED;
            return true;
        } finally {
            unlock();
        }
    }

    public boolean setErrored() {
        lock();
        try {
            if (status == ChoreStatus.FINISHED) return false;
            status = ChoreStatus.ERRORED;
            return true;
        } finally {
            unlock();
        }
    }

    public boolean setExpired() {
        lock();
        try {
            if (status == ChoreStatus.FINISHED) return false;
            status = ChoreStatus.EXPIRED;
            return true;
        } finally {
            unlock();
        }
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Chore");
        sb.append("{startTime=").append(startTime);
        sb.append(", key=").append(key);
        sb.append(", maxRunTimeMillis=").append(maxRunTimeMillis);
        sb.append(", conditions=").append(conditions);
        sb.append(", onFinish=").append(onFinish);
        sb.append(", onError=").append(onError);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }
}
