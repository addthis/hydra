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
package com.addthis.hydra.job;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.addthis.basis.net.HttpUtil;
import com.addthis.basis.util.Strings;
import com.addthis.basis.util.TokenReplacer;

import com.addthis.hydra.common.plugins.PluginReader;
import com.addthis.hydra.data.util.CommentTokenizer;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public class JobExpand {

    private static Logger log = LoggerFactory.getLogger(JobExpand.class);

    private static class MacroTokenReplacer extends TokenReplacer {

        private final Spawn.SpawnState spawnState;

        private static Logger log = LoggerFactory.getLogger(MacroTokenReplacer.class);

        MacroTokenReplacer(Spawn.SpawnState spawnState) {
            super("%{", "}%");
            this.spawnState = spawnState;
        }

        @Override
        public String replace(Region region, String label) {
            if (label.startsWith("http://")) {
                try {
                    return new String(HttpUtil.httpGet(label, 0).getBody(), "UTF-8");
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
            JobMacro macro = spawnState.macros.get(label);
            if (macro != null) {
                return macro.getMacro();
            } else {
                String msg = "non-existent macro referenced : " + label;
                log.warn(msg);
                throw new RuntimeException(msg);
            }
        }
    }

    // initialize optional/3rd party job config expanders
    private static Map<String, JobConfigExpander> expanders = new HashMap<>();

    static {
        List<String[]> expanders = PluginReader.readProperties("-jobexpand.classmap");
        for(String[] expander : expanders) {
            if (expander.length >= 2) {
                try {
                    registerExpander(expander[0], (Class<? extends JobConfigExpander>) Class.forName(expander[1]));
                } catch(ClassNotFoundException|ClassCastException e) {
                    log.warn(e.toString());
                }
            }
        }
    }

    static void registerExpander(String macroName, Class<? extends JobConfigExpander> clazz) {
        Object o = null;
        try {
            o = clazz.newInstance();
            expanders.put(macroName, (JobConfigExpander) o);
        } catch (InstantiationException | IllegalAccessException e) {
            log.warn("Class '" + clazz + "' registered for '" + macroName + "' cannot be initialized: " + e, e);
            } catch (ClassCastException e) {
            log.warn("Class '" + clazz + "' registered for '" + macroName + "' is not JobConfigExpander but '" + o.getClass() + "'");
        }
    }

    private static String macroTemplateParamsHelper(String input, final HashMap<String, String> map) {
        return new TokenReplacer("%[", "]%") {
            @Override
            public String replace(Region region, String label) {
                return map.get(Strings.splitArray(label, ":")[0]);
            }
        }.process(input);
    }

    static String macroTemplateParams(String expandedJob, Collection<JobParameter> params) {
        if (params != null && expandedJob != null) {
            final HashMap<String, String> map = new HashMap<>();
            for (JobParameter param : params) {
                String name = param.getName();
                String value = param.getValue();
                String defaultValue = param.getDefaultValue();
                map.put(name, Strings.isEmpty(value) ? defaultValue : value);
            }
            StringBuilder builder = new StringBuilder();
            List<String> contents = new ArrayList<>();
            List<String> delimiters = new ArrayList<>();
            CommentTokenizer commentTokenizer = new CommentTokenizer(expandedJob);
            commentTokenizer.tokenize(contents, delimiters);
            int length = contents.size();

            builder.append(macroTemplateParamsHelper(contents.get(0), map));
            String firstDelimiter = delimiters.get(0);
            if (firstDelimiter != "%[" && firstDelimiter != "]%") {
                builder.append(firstDelimiter);
            }
            for (int i = 1; i < length; i++) {
                String prevDelimiter = delimiters.get(i - 1);
                String nextDelimiter = delimiters.get(i);
                // Ignore parameters inside of comments
                if (prevDelimiter.equals("//") || prevDelimiter.equals("/*")) {
                    builder.append(contents.get(i));
                } else if (prevDelimiter.equals("%[") && nextDelimiter.equals("]%")) {
                    String value = map.get(Strings.splitArray(contents.get(i), ":")[0]);
                    if (value != null) {
                        builder.append(value);
                    }
                } else { // Delimiters such as double-quotes may contain parameters inside them
                    builder.append(macroTemplateParamsHelper(contents.get(i), map));
                }
                if (nextDelimiter != "%[" && nextDelimiter != "]%") {
                    builder.append(nextDelimiter);
                }
            }
            return builder.toString();
        }
        return expandedJob;
    }

    private static void addParameter(String paramString, Map<String, JobParameter> params) {
        JobParameter param = new JobParameter();
        String tokens[] = paramString.split(":", 2);
        param.setName(tokens[0]);
        if (tokens.length > 1) {
            param.setDefaultValue(tokens[1]);
        }
        /** re-declarations not allowed -- iow, first instance wins (for defaulting values) */
        if (params.get(param.getName()) == null) {
            params.put(param.getName(), param);
        }
    }

    private static void macroFindParametersHelper(String jobFragment, Map<String, JobParameter> params) {
        int index = 0;
        while (true) {
            int next = jobFragment.indexOf("%[", index);
            if (next >= 0) {
                int end = jobFragment.indexOf("]%", next + 2);
                if (end > 0) {
                    addParameter(jobFragment.substring(next + 2, end), params);
                    index = end + 2;
                } else {
                    index = next + 2;
                }
            } else {
                break;
            }
        }
    }

    /**
     * find parameters in the expanded job
     */
    public static Map<String, JobParameter> macroFindParameters(String expandedJob) {
        LinkedHashMap<String, JobParameter> params = new LinkedHashMap<>();
        if (expandedJob == null) {
            return params;
        }
        List<String> contents = new ArrayList<>();
        List<String> delimiters = new ArrayList<>();
        CommentTokenizer commentTokenizer = new CommentTokenizer(expandedJob);
        commentTokenizer.tokenize(contents, delimiters);
        int length = contents.size();

        macroFindParametersHelper(contents.get(0), params);
        for (int i = 1; i < length; i++) {
            String delimiter = delimiters.get(i - 1);
            // Ignore parameters inside of comments
            if (delimiter.equals("//") || delimiter.equals("/*")) {
                // do nothing
            } else if (delimiter.equals("%[") && delimiters.get(i).equals("]%")) {
                addParameter(contents.get(i), params);
            } else { // Delimiters such as double-quotes may contain parameters inside them
                macroFindParametersHelper(contents.get(i), params);
            }
        }
        return params;
    }

    /**
     * recursively expand macros
     */
    public static String macroExpand(Spawn spawn, String rawtext) {
        MacroTokenReplacer replacer = new MacroTokenReplacer(spawn.getSpawnState());
        List<String> contents = new ArrayList<>();
        List<String> delimiters = new ArrayList<>();
        CommentTokenizer commentTokenizer = new CommentTokenizer(rawtext);
        commentTokenizer.tokenize(contents, delimiters);
        StringBuilder builder = new StringBuilder();
        int length = contents.size();

        builder.append(replacer.process(contents.get(0)));
        builder.append(delimiters.get(0));
        for (int i = 1; i < length; i++) {
            String delimiter = delimiters.get(i - 1);
            if (delimiter.equals("//") || delimiter.equals("/*")) {
                builder.append(contents.get(i));
            } else {
                builder.append(replacer.process(contents.get(i)));
            }
            builder.append(delimiters.get(i));
        }
        return builder.toString();
    }

    /* special pass that injects spawn metadata and specific tokens
    * TODO - expand to include job shards option
    */
    public static String magicMacroExpand(final Spawn spawn, String rawtext, final String jobId) {
        return new TokenReplacer("%(", ")%") {
            @Override
            public String replace(Region region, String label) {
                List<String> mfn = Lists.newArrayList(Splitter.on(' ').split(label));
                String macroName = mfn.get(0);
                List<String> tokens = mfn.subList(1, mfn.size());
                if (macroName.equals("jobhosts")) {
                    JobMacro macro = spawn.createJobHostMacro(tokens.get(0), Integer.parseInt(tokens.get(1)));
                    return macro.getMacro();
                } else if (expanders.containsKey(macroName)) {
                    return expanders.get(macroName).expand(spawn.getSpawnDataStore(), jobId, tokens);
                } else {
                    String msg = "non-existent magic macro referenced : " + label;
                    log.warn(msg);
                    throw new RuntimeException(msg);
                }
            }
        }.process(rawtext);
    }
}
