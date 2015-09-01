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

import java.text.DecimalFormat;

import com.google.common.base.Strings;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotTest {
    private static final Logger log = LoggerFactory.getLogger(SnapshotTest.class);

    private static final DecimalFormat percent = new DecimalFormat("0.0% ");

    @Ignore("just for tinkering")
    @Test public void formatting() throws Exception {
        log.info(Strings.padStart(percent.format(1.0), 6, ' '));
        log.info(percent.format(0.9));
        log.info(percent.format(1.1));
        log.info(percent.format(0.01));
    }
}