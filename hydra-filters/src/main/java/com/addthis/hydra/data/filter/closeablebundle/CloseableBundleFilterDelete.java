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
package com.addthis.hydra.data.filter.closeablebundle;

import java.io.File;
import java.io.IOException;

import com.addthis.bundle.core.Bundle;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.SuperCodable;

import org.apache.commons.io.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CloseableBundleFilterDelete implements SuperCodable, CloseableBundleFilter {
    private static final Logger log = LoggerFactory.getLogger(CloseableBundleFilterDelete.class);

    @FieldConfig(codable = true, required = true)
    private String fileName;
    @FieldConfig(codable = true)
    private boolean pre = false;

    private CloseableBundleFilterDelete() {}

    @Override public void postDecode() {
        if (pre) {
            delete(fileName);
        }
    }

    @Override public void preEncode() {}

    @Override
    public void close() {
        if (!pre) {
            delete(fileName);
        }
    }

    private void delete(String fileName) {
        if (fileName != null) {
            File file = new File(fileName);

            if (file.exists()) {
                try {
                    FileUtils.deleteDirectory(file);
                    log.warn("Deleted file : " + fileName);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public boolean filter(Bundle row) {
        return true;
    }

}
