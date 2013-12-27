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
package com.addthis.hydra.common.plugins;

import java.util.List;
import java.util.Map;

import org.springframework.core.io.Resource;

public class TestPlugins {

    public static void main(String[] args) {
        Map<Resource, List<String[]>> mapping = PluginReader.readPropertiesAndMap(".classmap");
        if (mapping.size() == 0) {
            System.err.println("No classmap files were found on the classpath.");
            System.exit(1);
        }
        int classes = 0;
        boolean error = false;
        for(Map.Entry<Resource, List<String[]>> entry : mapping.entrySet()) {
            Resource resource = entry.getKey();
            List<String[]> rows = entry.getValue();
            for(String[] row : rows) {
                if (row.length >= 2) {
                    try {
                        Class.forName(row[1]);
                        classes++;
                    } catch (ClassNotFoundException e) {
                        System.err.println("In resource " + resource.getDescription() + ": " + e.toString());
                        error = true;
                    }
                }
            }
        }
        if (error) {
            System.exit(2);
        }
        System.out.println("Located " + classes + " classes specified in " + mapping.size() + " classmap files.");
    }

}
