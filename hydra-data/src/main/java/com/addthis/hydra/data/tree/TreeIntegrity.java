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
package com.addthis.hydra.data.tree;

import java.io.File;
import java.io.IOException;

import com.addthis.hydra.data.tree.concurrent.ConcurrentTree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeIntegrity {

    private static final Logger log = LoggerFactory.getLogger(TreeIntegrity.class);

    public static void main(String[] args) throws IOException {
        File root = new File(args[0]);
        if (args.length > 1 && args[1].equals("repair")) {
            ConcurrentTree tree = null;
            try {
                tree = new ConcurrentTree(root);
                tree.repairIntegrity();
            } catch (Exception ex) {
                log.error(ex.toString());
            } finally {
                if (tree != null) tree.close();
            }
        } else {
            ReadTree tree = null;
            try {
                tree = new ReadTree(root);
                tree.testIntegrity();
            } catch(Exception ex) {
                log.error(ex.toString());
            } finally {
                if (tree != null) tree.close();
            }
        }
    }


}
