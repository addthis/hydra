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
package com.addthis.hydra.data.query.op;

import java.util.Random;

import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.query.AbstractRowOp;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">randomly fails</span>.
 * <p>The operation syntax is "rndfail=N" where N is the percentage of failure
 * on a per row basis.
 *
 * @user-reference
 * @hydra-name rndfail
 */
public class OpRandomFail extends AbstractRowOp {

    private static final Random random = new Random(System.currentTimeMillis());

    public OpRandomFail(String arg, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        percent = Integer.parseInt(arg);
    }

    private int percent;

    @Override
    public Bundle rowOp(Bundle row) {
        if (random.nextInt(100) < percent) {
            throw new RuntimeException("luck-o-the-draw error");
        }
        return row;
    }

}
