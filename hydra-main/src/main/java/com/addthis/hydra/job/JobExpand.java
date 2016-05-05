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
import java.util.regex.Pattern;

import com.addthis.basis.net.HttpUtil;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.LessStrings;
import com.addthis.basis.util.TokenReplacer;
import com.addthis.basis.util.TokenReplacerOverflowException;

import com.addthis.codec.plugins.PluginMap;
import com.addthis.codec.plugins.PluginRegistry;
import com.addthis.hydra.data.util.CommentTokenizer;
import com.addthis.hydra.job.alias.AliasManager;
import com.addthis.hydra.job.entity.JobEntityManager;
import com.addthis.hydra.job.entity.JobMacro;
import com.addthis.hydra.job.entity.JobMacroManager;
import com.addthis.hydra.job.spawn.Spawn;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

public class JobExpand {

    private static final int maxDepth = Parameter.intValue("spawn.macro.expand.depth", 256);

    private static final Logger log = LoggerFactory.getLogger(JobExpand.class);

    private static class MacroTokenReplacer extends TokenReplacer {

        private static final Logger log = LoggerFactory.getLogger(MacroTokenReplacer.class);

        private static final Joiner joiner = Joiner.on(',').skipNulls();

        private static final Pattern macroPattern = Pattern.compile("%\\{(.+?)\\}%");
        private final JobEntityManager<JobMacro> jobMacroManager;
        private final AliasManager aliasManager;

        MacroTokenReplacer(@Nonnull JobEntityManager<JobMacro> jobMacroManager, @Nonnull AliasManager aliasManager) {
            super("%{", "}%");
            this.jobMacroManager = jobMacroManager;
            this.aliasManager = aliasManager;
        }

        @Override
        public long getMaxDepth() {
            return maxDepth;
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
            JobMacro macro = jobMacroManager.getEntity(label);
            String target = null;
            if (macro != null) {
                target = macro.getMacro();
            } else {
                List<String> aliases = aliasManager.aliasToJobs(label);
                if (aliases != null) {
                    target = joiner.join(aliases);
                }
            }
            if (target != null) {
                List<String> contents = new ArrayList<>();
                List<String> delimiters = new ArrayList<>();
                CommentTokenizer commentTokenizer = new CommentTokenizer(target);
                commentTokenizer.tokenize(contents, delimiters);
                StringBuilder builder = new StringBuilder();
                int length = contents.size();

                builder.append(contents.get(0));
                builder.append(delimiters.get(0));
                for (int i = 1; i < length; i++) {
                    String delimiter = delimiters.get(i - 1);
                    if (delimiter.equals("//") || delimiter.equals("/*")) {
                        /* disable any macros inside comments so they don't get expanded */
                        builder.append(macroPattern.matcher(contents.get(i)).replaceAll("%_{$1}_%"));
                    } else {
                        builder.append(contents.get(i));
                    }
                    builder.append(delimiters.get(i));
                }
                return builder.toString();
            } else {
                String msg = "non-existent macro referenced : " + label;
                log.warn(msg);
                throw new RuntimeException(msg);
            }
        }
    }

    // initialize optional/3rd party job config expanders
    private static final Map<String, JobConfigExpander> expanders = new HashMap<>();

    static {
        PluginMap expanderMap = PluginRegistry.defaultRegistry().asMap().get("job expander");
        if (expanderMap != null) {
            for (Map.Entry<String, Class<?>> expanderPlugin : expanderMap.asBiMap().entrySet()) {
                registerExpander(expanderPlugin.getKey(),
                                 (Class<? extends JobConfigExpander>) expanderPlugin.getValue());
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

    private static String macroTemplateParamsHelper(String input, final HashMap<String, String> map)
            throws TokenReplacerOverflowException {
        return new TokenReplacer("%[", "]%") {
            @Override
            public String replace(Region region, String label) {
                return map.get(LessStrings.splitArray(label, ":")[0]);
            }

            @Override
            public long getMaxDepth() {
                return maxDepth;
            }
        }.process(input);
    }

    public static String macroTemplateParams(String expandedJob, Collection<JobParameter> params)
            throws TokenReplacerOverflowException {
        if (params != null && expandedJob != null) {
            final HashMap<String, String> map = new HashMap<>();
            for (JobParameter param : params) {
                String name = param.getName();
                map.put(name, param.getValueOrDefault());
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
                    String value = map.get(LessStrings.splitArray(contents.get(i), ":")[0]);
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
        String[] tokens = paramString.split(":", 2);
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
     * 
     * @throws IllegalStateException if expanded config exceeds the max length allowed.
     */
    public static String macroExpand(@Nonnull JobEntityManager<JobMacro> macroManager,
                                     @Nonnull AliasManager aliasManager,
                                     @Nonnull String rawtext) throws TokenReplacerOverflowException, IllegalStateException {
        MacroTokenReplacer replacer = new MacroTokenReplacer(macroManager, aliasManager);
        List<String> contents = new ArrayList<>();
        List<String> delimiters = new ArrayList<>();
        CommentTokenizer commentTokenizer = new CommentTokenizer(rawtext);
        commentTokenizer.tokenize(contents, delimiters);
        StringBuilder builder = new StringBuilder();
        int length = contents.size();

        builder.append(replacer.process(contents.get(0)));
        builder.append(delimiters.get(0));
        for (int i = 1; i < length; i++) {
            if (builder.length() > Spawn.INPUT_MAX_NUMBER_OF_CHARACTERS) {
                throw new IllegalStateException("Expanded job config length of at least " + builder.length()
                                                   + " characters is greater than max length of "
                                                   + Spawn.INPUT_MAX_NUMBER_OF_CHARACTERS);
            }
            String delimiter = delimiters.get(i - 1);
            if (delimiter.equals("//") || delimiter.equals("/*")) {
                builder.append(contents.get(i));
            } else {
                builder.append(replacer.process(contents.get(i)));
            }
            builder.append(delimiters.get(i));
        }
        if (builder.length() > Spawn.INPUT_MAX_NUMBER_OF_CHARACTERS) {
            throw new IllegalStateException("Expanded job config length of " + builder.length()
                                            + " characters is greater than max length of "
                                            + Spawn.INPUT_MAX_NUMBER_OF_CHARACTERS);
        }
        return builder.toString();
    }

    /* special pass that injects spawn metadata and specific tokens
    * TODO - expand to include job shards option
    */
    public static String magicMacroExpand(final Spawn spawn, String rawtext, final String jobId)
            throws TokenReplacerOverflowException {
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

            @Override
            public long getMaxDepth() {
                return maxDepth;
            }
        }.process(rawtext);
    }
}
