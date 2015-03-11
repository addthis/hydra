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
package com.addthis.hydra.task.output;

import java.io.IOException;

import com.addthis.bundle.core.Bundle;
import com.addthis.codec.annotations.Pluggable;

/**
 * Interface for classes capable of [re]opening {@link OutputWrapper}s.
 */
@Pluggable("output-factory")
public interface OutputWrapperFactory extends WritableRootPaths {

    /**
     * Open a new or reopen an existing {@link OutputWrapper} and return a reference to that object
     *
     * @param target the raw target name for the output stream
     * @param outputFlags {@link OutputStreamFlags} for controlling output behavior
     * @param streamEmitter emitter to convert {@link Bundle}s into bytes
     * @return a reference to a {@link OutputWrapper} instance
     * @throws IOException
     */
    OutputWrapper openWriteStream(String target,
                                  OutputStreamFlags outputFlags,
                                  OutputStreamEmitter streamEmitter) throws IOException;
}
