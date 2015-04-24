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
package com.addthis.hydra.job;

import com.addthis.hydra.minion.Minion;
import com.addthis.hydra.job.spawn.Spawn;

/**
 * command-line/jar entry-point to start either spawn or minion.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            if (args[0].equals("spawn")) {
                Spawn.main(cutargs(args));
                return;
            } else if (args[0].equals("minion")) {
                Minion.main(cutargs(args));
                return;
            }
        }
        usage();
    }

    private static String[] cutargs(String[] args) {
        String[] ns = new String[args.length - 1];
        System.arraycopy(args, 1, ns, 0, args.length - 1);
        return ns;
    }

    private static void usage() {
        System.out.println("usage: batch [ spawn | minion ] <args>");
    }
}
