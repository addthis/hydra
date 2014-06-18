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
package com.addthis.hydra.store.db;

public enum CloseOperation {

    NONE(false, false), TEST(true, false), REPAIR(true, true);

    private final boolean test;
    private final boolean repair;

    CloseOperation(boolean test, boolean repair) {
        this.test = test;
        this.repair = repair;
    }

    public boolean testIntegrity() {
        return test;
    }

    public boolean repairIntegrity() {
        return repair;
    }


}
