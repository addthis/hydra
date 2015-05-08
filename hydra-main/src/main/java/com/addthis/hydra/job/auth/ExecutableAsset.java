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
package com.addthis.hydra.job.auth;

public interface ExecutableAsset extends OwnableAsset {

    boolean isOwnerExecutable();

    boolean isGroupExecutable();

    boolean isWorldExecutable();

    /**
     * @throws UnsupportedOperationException if asset view is read-only
     */
    void setOwnerExecutable(boolean executable);

    /**
     * @throws UnsupportedOperationException if asset view is read-only
     */
    void setGroupExecutable(boolean executable);

    /**
     * @throws UnsupportedOperationException if asset view is read-only
     */
    void setWorldExecutable(boolean executable);

}
