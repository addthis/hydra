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
package com.addthis.hydra.task.source;

import com.addthis.codec.Codec;

import com.google.common.base.Objects;

/**
 * file mark record
 */
public final class Mark extends SimpleMark {

    @Codec.Set(codable = true)
    private int error;

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("value", getValue())
                .add("error", getError())
                .add("index", getIndex())
                .add("end", isEnd())
                .toString();
    }

    public int getError() {
        return error;
    }

    public void setError(int error) {
        this.error = error;
    }
}
