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
import com.addthis.codec.Codec; import com.addthis.codec.annotations.FieldConfig;

import org.apache.commons.io.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CloseableBundleFilterDelete extends CloseableBundleFilter {

    @FieldConfig(codable = true, required = true)
    private String fileName;
    @FieldConfig(codable = true)
    private boolean pre = false;

    private Logger log = LoggerFactory.getLogger(CloseableBundleFilterDelete.class);

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
    public void initialize() {
        if (pre) {
            delete(fileName);
        }
    }

    @Override
    public boolean filterExec(Bundle row) {
        return true;
    }
}
