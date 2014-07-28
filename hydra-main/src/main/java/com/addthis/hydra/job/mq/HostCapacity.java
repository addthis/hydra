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

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;


public class HostCapacity implements Codable, Serializable {

    private static final long serialVersionUID = -8791328421008641045L;

    @FieldConfig(codable = true)
    private int mem;
    @FieldConfig(codable = true)
    private int cpu;
    @FieldConfig(codable = true)
    private int io;
    @FieldConfig(codable = true)
    private long disk;

    public HostCapacity() {
    }

    public HostCapacity(int mem, int cpu, int io) {
        this(mem, cpu, io, 0);
    }

    public HostCapacity(int mem, int cpu, int io, long disk) {
        this.mem = mem;
        this.cpu = cpu;
        this.io = io;
        this.disk = disk;
    }

    public void add(HostCapacity cap) {
        mem += cap.mem;
        cpu += cap.cpu;
        io += cap.io;
    }

    public int getMem() {
        return mem;
    }

    public int getCpu() {
        return cpu;
    }

    public int getIo() {
        return io;
    }

    public long getDisk() {
        return disk;
    }

    public void setMem(int mem) {
        this.mem = mem;
    }

    public void setCpu(int cpu) {
        this.cpu = cpu;
    }

    public void setIo(int io) {
        this.io = io;
    }

    public void setDisk(long disk) {
        this.disk = disk;
    }
}
