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
package com.addthis.hydra.util;

import com.addthis.basis.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class PreLoader {
    private static final Logger log = LoggerFactory.getLogger(PreLoader.class);
    private static boolean hasRun = false;

    public static void find() {
        if (hasRun) {
            return;
        }
        hasRun = true;
        HashMap<String,ArrayList<URL>> urlSet = new HashMap<String,ArrayList<URL>>();
        HashMap<String,String> execSet = new HashMap<String,String>();
        for (Map.Entry<Object, Object> e : System.getProperties().entrySet()) {
            String key = (String)e.getKey();
            String val = (String)e.getValue();
            try {
                // form --> hydra-preload-[groupkey]-[class]=[jarurl]
                if (key.startsWith("hydra-preload-")) {
                    String args[] = Strings.splitArray(key,"-");
                    if (args.length < 3) {
                        continue;
                    }
                    key = args[2];
                    if (args.length > 3) {
                        execSet.put(key, args[3]);
                    }
                    ArrayList<URL> urls = urlSet.get(key);
                    if (urls == null) {
                        urls = new ArrayList<URL>();
                        urlSet.put(key, urls);
                    }
                    urls.add(new URL(val));
                }
                // form --> hydra-preexec-[groupkey]=[classname]
                if (key.startsWith("hydra-preexec-")) {
                    String args[] = Strings.splitArray(key,"-");
                    if (args.length < 3) {
                        continue;
                    }
                    key = args[2];
                    execSet.put(key, val);
                }
            } catch (Exception ex) {
                log.debug("bad url", ex);
            }
        }
        for (Map.Entry<String, ArrayList<URL>> e : urlSet.entrySet()) {
            String key = e.getKey();
            ArrayList<URL> urls = e.getValue();
            URL array[] = urls.toArray(new URL[urls.size()]);
            URLClassLoader cl = new URLClassLoader(array);
            try {
                ((PreLoader)(cl.loadClass(execSet.get(key)).newInstance())).exec();
            } catch (Exception ex) {
                log.debug("failed init", ex);
            }
        }
    }

    public abstract void exec();

}
