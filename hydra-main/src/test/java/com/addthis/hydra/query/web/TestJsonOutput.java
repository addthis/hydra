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
package com.addthis.hydra.query.web;

import com.addthis.hydra.query.web.OutputJson;

import org.junit.Test;

public class TestJsonOutput {

    @Test
    public void encode() {
        String s1 = "abc\"def\'ghi\\jkl\nmno\tpqr";
        String e1 = OutputJson.jsonEncode(s1);
        System.out.println("s1 = " + s1);
        System.out.println("e1 = " + e1);
    }
}
