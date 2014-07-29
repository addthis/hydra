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
package com.addthis.hydra.util;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.hydra.task.output.TaskDataOutput;

import org.junit.Ignore;
import org.junit.Test;

public class LogUtilTest {

    @Test @Ignore
    public void newBundleOutput() {
        TaskDataOutput eventLog = LogUtil.newBundleOutputFromConfig("logtest");
        Bundle event = eventLog.createBundle();
        BundleFormat eventFormat = event.getFormat();
        BundleField typeField = eventFormat.getField("TYPE");
        event.setValue(typeField, ValueFactory.create("verbose"));
        BundleField messageField = eventFormat.getField("MESSAGE");
        event.setValue(messageField, ValueFactory.create("I am a \"super \'cool message"));
        eventLog.send(event);
        eventLog.send(event);
        eventLog.send(event);
        eventLog.sendComplete();
    }
}
