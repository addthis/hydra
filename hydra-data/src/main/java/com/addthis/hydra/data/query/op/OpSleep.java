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

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.query.AbstractRowOp;


/**
 * <p>This query operation <span class="hydra-summary">sleeps for a number of milliseconds</span>.
 * <p/>
 * <p>There are two forms of this operation. "sleep=N" sleeps for N milliseconds.
 * "range=N,M" sleeps for N milliseconds plus a random number of milliseconds drawn
 * from a uniform distribution on the range [- M/2, + M/2].
 *
 * @user-reference
 * @hydra-name sleep
 */
public class OpSleep extends AbstractRowOp {

    private static final Random rnd = new Random(System.currentTimeMillis());

    public OpSleep(String arg) {
        String op[] = Strings.splitArray(arg, ",");
        sleep = Long.parseLong(op[0]);
        random = op.length > 1 ? Long.parseLong(op[1]) : 0;
    }

    private final long sleep;
    private final long random;

    @Override
    public Bundle rowOp(Bundle row) {
        try {
            long time = sleep;
            if (random > 0) {
                Math.abs(time += (rnd.nextDouble() * random) - (random / 2));
            }
            Thread.sleep(time);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return row;
    }

}
