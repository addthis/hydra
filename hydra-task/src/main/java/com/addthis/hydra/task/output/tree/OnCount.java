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
package com.addthis.hydra.task.output.tree;

import com.addthis.codec.Codec;

/**
 * This class represents possible triggering value changes.
 * anyof is a list of values to match.
 * match is a single value to match.
 * mad is matched whenever the value % mod == 0
 */
public final class OnCount implements Codec.Codable {

    @Codec.Set(codable = true)
    private long anyof[];
    @Codec.Set(codable = true)
    private long match;
    @Codec.Set(codable = true)
    private long mod;
    @Codec.Set(codable = true)
    private PathElement exec;

    public void resolve(TreeMapper mapper) {
        exec.resolve(mapper);
    }

    public void exec(TreeMapState state) {
        if (exec != null) {
            try {
                exec.processNode(state);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isMatch(long v1, long v2) {
        if (anyof != null) {
            for (long v : anyof) {
                if (v1 < v && v2 >= v) {
                    return true;
                }
            }
        }
        if (match > 0 && v1 < match && v2 >= match) {
            return true;
        }
        if (v2 > 0 && mod > 0) {
            if (v2 - v1 > mod) {
                return true;
            }
            long m2 = v2 % mod;
            if (m2 == 0) {
                return true;
            }
            long m1 = v1 % mod;
            return m1 > m2;
        }
        return false;
    }

    public boolean isMatch(long v1) {
        if (anyof != null) {
            for (long v : anyof) {
                if (v1 == v) {
                    return true;
                }
            }
        }
        if (match > 0 && v1 == match) {
            return true;
        }
        if (mod > 0) {
            return v1 % mod == 0;
        }
        return false;
    }
}
