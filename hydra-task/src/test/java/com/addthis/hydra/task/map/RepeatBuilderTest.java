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
package com.addthis.hydra.task.map;

import java.io.IOException;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.Bundles;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.value.ValueTranslationException;

import org.junit.Before;
import org.junit.Test;

import static com.addthis.codec.config.Configs.decodeObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RepeatBuilderTest {

    StreamEmitter emitter;

    @Before
    public void setup() {
        emitter = mock(StreamEmitter.class);
    }

    @Test
    public void basicFunctionality() throws IOException {
        int reps = 5;
        RepeatBuilder builder = decodeObject(RepeatBuilder.class, "repeatField: reps");
        Bundle bundle = Bundles.decode("reps: " + reps);
        builder.process(bundle, emitter);
        verify(emitter, times(reps)).emit(bundle);
    }

    @Test
    public void defaultRepeatCount() throws IOException {
        int defaultRepeatCount = 2;
        RepeatBuilder builder = decodeObject(RepeatBuilder.class,
                                             "repeatField: reps, defaultRepeats: " + defaultRepeatCount);
        Bundle bundle = new ListBundle();
        builder.process(bundle, emitter);
        verify(emitter, times(defaultRepeatCount)).emit(bundle);
    }

    @Test(expected = ValueTranslationException.class)
    public void escalateOnParseFailure() throws IOException {
        RepeatBuilder builder = decodeObject(RepeatBuilder.class, "repeatField: reps, failOnParseException: true");
        Bundle bundle = Bundles.decode("reps: zzzzzz");
        builder.process(bundle, emitter);
    }
}
