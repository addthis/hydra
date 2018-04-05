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
package com.addthis.hydra.job.entity;

import java.util.Arrays;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.json.CodecJSON;
import com.addthis.maljson.JSONObject;

import com.google.common.base.MoreObjects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * command run on minion to start each task in a job
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
public final class JobCommand implements Codable, Cloneable {

    @FieldConfig
    private String owner;
    @FieldConfig
    private String[] command;
    @FieldConfig
    private int reqCPU;
    @FieldConfig
    private int reqMEM;
    @FieldConfig
    private int reqIO;

    public JobCommand() {
    }

    public JobCommand(String owner, String[] command, int cpu, int mem, int io) {
        this.owner = owner;
        this.command = command;
        this.reqCPU = cpu;
        this.reqMEM = mem;
        this.reqIO = io;
    }

    public String getOwner() {
        return owner;
    }

    public String[] getCommand() {
        return command;
    }

    public int getReqCPU() {
        return reqCPU;
    }

    public int getReqMEM() {
        return reqMEM;
    }

    public int getReqIO() {
        return reqIO;
    }


    public JSONObject toJSON() throws Exception {
        return CodecJSON.encodeJSON(this);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("owner", owner)
                .add("command", Arrays.deepToString(command))
                .add("reqCPU", reqCPU)
                .add("reqMEM", reqMEM)
                .add("reqIO", reqIO)
                .toString();
    }
}
