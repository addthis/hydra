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
package com.addthis.hydra.data.filter.value;

import java.io.File;

import com.addthis.basis.test.SlowTest;
import com.addthis.basis.util.LessFiles;

import com.addthis.codec.json.CodecJSON;

import com.google.common.io.Files;

import org.junit.Test;
import org.junit.experimental.categories.Category;
@Category(SlowTest.class)
public class TestValueFilterHttpGet {

    @Test
    public void simpleReplace() {
        File tmpDirLocation = Files.createTempDir();
        try {
            String tmpDir = tmpDirLocation.toString();

            // FIXME: neuon or some other 'traditional' test domain
            ValueFilterHttpGet filter = CodecJSON.decodeString(new ValueFilterHttpGet(),
                                                               "{cacheSize:5,cacheAge:10000," +
                                                               "persist:true,persistDir:'" +
                                                               tmpDir +
                                                               "',template:'http://www.google.com/search?sclient=psy&hl=en&site=&source=hp&q={{}}&btnG=Search'}");
            for (int i = 0; i < 5; i++) {
                String search = filter.filter("hello+world+" + i);
                System.out.println("#" + i + " >> " + (search != null ? search.length() : "null"));
            }
            for (int i = 0; i < 10; i++) {
                String search = filter.filter("hello+world+" + i);
                System.out.println("#" + i + " >> " + (search != null ? search.length() : "null"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (tmpDirLocation != null) {
                LessFiles.deleteDir(tmpDirLocation);
            }
        }
    }
}
