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
package com.addthis.hydra.task.run;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;

import java.net.URL;
import java.net.URLClassLoader;

import java.util.HashSet;
import java.util.Set;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;

import com.addthis.codec.json.CodecJSON;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;


/**
 * Main method invoked when running tasks.
 * <p/>
 * "usage: run <config> <nodes> <node> [jobid] [threads]"
 */
public class JsonRunner {

    static boolean checkArgs(String[] args) {
        if (args.length < 2) {
            System.out.println("usage: run <config> <nodes> <node> [jobid] [threads]");
            return false;
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        if (!checkArgs(args)) return;
        String fileName = args[0];
        String configString = TaskRunner.loadStringFromFile(fileName);
        runTask(configString, args);
    }

    static void runTask(String config, String[] args) throws Exception {
        if (!checkArgs(args)) return;
        int nodeCount = Integer.parseInt(args[1]);
        int thisNode = Integer.parseInt(args[2]);
        String jobId = (args.length > 3) ? args[3] : null;
        int commandLineThreads = (args.length > 4) ? Integer.parseInt(args[4]) : TaskRunner.defaultThreads;
        runTask(config, nodeCount, thisNode, jobId, commandLineThreads);
    }

    static void runTask(String configString, int nodeCount, int thisNode,
            String jobId, int commandLineThreads) throws Exception {

        String json = subAt(configString);
        JSONObject jo;
        try {
            jo = new JSONObject(json);
        } catch (JSONException ex) {
            if ((ex.getColumn() == 0) && (ex.getLine() == 0)) {
                HoconRunner.runTask(json, nodeCount, thisNode, jobId, commandLineThreads);
                return;
            } else {
                throw ex;
            }
        }
        preload(jo);

        initClasses(jo);
        final TaskRunnable task = CodecJSON.decodeObject(TaskRunnable.class, jo);
        TaskRunConfig config = new TaskRunConfig(thisNode,
                nodeCount, jobId);
        config.setThreadCount(jo.optInt("taskthreads", commandLineThreads));
        task.init(config);
        task.exec();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                task.terminate();
                task.waitExit();
            }
        });

    }

    public static TaskRunnable loadConfig(File config) throws Exception {
        JSONObject jo = new JSONObject(subAt(Bytes.toString(Files.read(config))));
        return CodecJSON.decodeObject(TaskRunnable.class, jo);
    }

    public static void initClasses(JSONObject jsonObject) {

        try {
            JSONArray classes = jsonObject.getJSONArray("classes");
            if (classes != null && classes.length() > 0) {
                ClassLoader classLoader = JsonRunner.class.getClassLoader();
                for (int i = 0; i < classes.length(); i++) {
                    String classToLoad = classes.getString(i);
                    System.out.println("loading class '" + classToLoad + "'");
                    Class<?> clazz = classLoader.loadClass(classToLoad);
                    clazz.newInstance();
                }

            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (JSONException e) {
            // swallow
        }
    }

    interface TaskStringReplacement {

        /**
         * Either perform a transformation on the string or return
         * the input object as output indicating that no transformation
         * was performed.
         */
        public abstract @Nonnull String replace(@Nonnull String input) throws IOException;
    }

    private static final Set<TaskStringReplacement> replaceOperators = new HashSet<>();

    static {
        replaceOperators.add(new TaskReplacementFile());
        replaceOperators.add(new TaskReplacementZoo());
    }

    /**
     * replace references with file contents
     */
    static String subAt(String json) throws IOException {
        boolean transformed;
        String output = json;

        do {
            String begin = output;
            for(TaskStringReplacement replacement : replaceOperators) {
                output = replacement.replace(output);
            }
            transformed = (begin != output);
        } while(transformed);

        return output;
    }

    /**
     * preload classes and jars required to resolve this config
     */
    static void preload(JSONObject o) throws Exception {
        JSONArray jars = o.optJSONArray("jars");
        JSONArray classes = o.optJSONArray("jar-classes");
        // load jars and classes
        if (jars != null && classes != null) {
            URL[] u = new URL[jars.length()];
            for (int i = 0; i < u.length; i++) {
                u[i] = new URL(jars.getString(i));
            }
            URLClassLoader cl = new URLClassLoader(u);
            for (int i = 0; i < classes.length(); i++) {
                cl.loadClass(classes.getString(i)).newInstance();
            }
        }
    }
}
