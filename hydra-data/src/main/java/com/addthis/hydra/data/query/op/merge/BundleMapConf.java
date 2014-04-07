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

package com.addthis.hydra.data.query.op.merge;

import com.addthis.bundle.core.BundleField;

public class BundleMapConf<K> {

    private K op;
    private BundleField from;
    private BundleField to;

    public K getOp() {
        return op;
    }

    public void setOp(K op) {
        this.op = op;
    }

    public BundleField getFrom() {
        return from;
    }

    public void setFrom(BundleField from) {
        this.from = from;
    }

    public BundleField getTo() {
        return to;
    }

    public void setTo(BundleField to) {
        this.to = to;
    }
}
