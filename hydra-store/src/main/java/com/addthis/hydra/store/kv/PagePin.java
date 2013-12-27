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
package com.addthis.hydra.store.kv;

/**
 * object that refers to a page and prevents it from being evited
 * until all pins are released
 */
public interface PagePin {

    /**
     * must be called exactly once for each pin acquired
     *
     * @throws IllegalStateException if called more than once
     */
    public void release() throws IllegalStateException;
}
