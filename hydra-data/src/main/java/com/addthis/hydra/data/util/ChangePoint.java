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
package com.addthis.hydra.data.util;

import com.addthis.codec.Codec;

/**
 *         A class representing a significant change in an array of integers
 */
public class ChangePoint implements Codec.Codable {

    private long size;
    private int index;
    private ChangePointType type;

    public ChangePoint(long size, int index, ChangePointType type) {
        this.size = size;
        this.index = index;
        this.type = type;
    }

    public long getSize() {
        return size;
    }

    public int getIndex() {
        return index;
    }

    public ChangePointType getType() {
        return type;
    }


    public enum ChangePointType {RISE, FALL, START, STOP, PEAK}
}
