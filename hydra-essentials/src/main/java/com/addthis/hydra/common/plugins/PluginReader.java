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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.addthis.codec.Codec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

public class PluginReader {

    private static final Logger log = LoggerFactory.getLogger(PluginReader.class);

    private static final PathMatchingResourcePatternResolver resolver =
            new PathMatchingResourcePatternResolver();

    private static final CsvPreference csvParserOptions =
            new CsvPreference.Builder(CsvPreference.STANDARD_PREFERENCE)
            .surroundingSpacesNeedQuotes(true)
            .build();

    /**
     * Reads all of the resource files under the specified path
     * and returns a sequence of String arrays. The resource files
     * are specified as comma-separated values. Each row in a resource
     * file is one element of the resulting list.
     *
     * @param suffix filename suffix. Matches against files with this suffix in their filename.
     * @return list of String[] elements that represent CSV values
     */
    public static List<String[]> readProperties(@Nonnull String suffix) {
        List<String[]> result = new ArrayList<>();
        try {
            String locationPattern = "classpath*:plugins/*" + suffix;
            Resource[] resources = resolver.getResources(locationPattern);
            for (Resource resource : resources) {
                log.debug("readProperties found resource {}", resource);
                String filename = resource.getDescription();
                InputStreamReader reader = new InputStreamReader(resource.getInputStream());
                try (CsvListReader csvParser = new CsvListReader(reader, csvParserOptions)) {
                    List<String> next;
                    while ((next = csvParser.read()) != null) {
                        result.add(next.toArray(new String[next.size()]));
                    }
                } catch (SuperCsvException e) {
                    log.warn("In " + filename + ", " + e);
                }
            }
        } catch (IOException e) {
            log.error(e.toString());
        }
        return result;
    }

    /**
     * Reads all of the resource files under the specified path
     * and returns a mapping of the resource to the corresponding
     * sequence of String arrays. The resource files
     * are specified as comma-separated values. Each row in a resource
     * file is one element of the resulting list.
     *
     * @param suffix filename suffix. Matches against files with this suffix in their filename.
     * @return list of String[] elements that represent CSV values
     */
    public static Map<Resource, List<String[]>> readPropertiesAndMap(@Nonnull String suffix) {
        Map<Resource, List<String[]>> result = new HashMap<>();
        try {
            String locationPattern = "classpath*:plugins/*" + suffix;
            Resource[] resources = resolver.getResources(locationPattern);
            for (Resource resource : resources) {
                List<String[]> list = new ArrayList<>();
                String filename = resource.getDescription();
                InputStreamReader reader = new InputStreamReader(resource.getInputStream());
                try (CsvListReader csvParser = new CsvListReader(reader, csvParserOptions)) {
                    List<String> next;
                    while ((next = csvParser.read()) != null) {
                        list.add(next.toArray(new String[next.size()]));
                    }
                } catch (SuperCsvException e) {
                    log.error("In " + filename + ", " + e);
                }
                result.put(resource, list);
            }
        } catch (IOException e) {
            log.warn(e.toString());
        }
        return result;
    }

    /**
     * Reads all the the properties that match the specified suffix
     * and load them into the class map.
     *
     * @param suffix
     * @param map
     * @param parentClass
     */
    public static void registerPlugin(@Nonnull String suffix,
            @Nonnull Codec.ClassMap map, @Nonnull Class parentClass) {
        List<String[]> filters = PluginReader.readProperties(suffix);
        for (String[] filter : filters) {
            if (filter.length >= 2) {
                Class clazz = loadClass(suffix, filter[0],
                        filter[1], parentClass, null);
                map.add(filter[0], clazz);
            }
        }
    }

    private static Class loadClassHelper(String suffix, String key,
            String className, Class parentClass, ClassLoader loader, boolean ignoreError) {
        try {
            Class clazz = Class.forName(className, true, loader);
            if (parentClass.isAssignableFrom(clazz)) {
                return clazz;
            } else {
                log.warn("For key \"{}\" the corresponding class {} is not a subtype of {}",
                        key, className, parentClass.getCanonicalName());
            }
        } catch (ClassNotFoundException e) {
            if (!ignoreError) {
                log.warn("registerPlugin failure. File suffix is \"{}\"," +
                         " key is \"{}\", exception is {}.",
                        suffix, key, e.toString());
            }
        }
        return null;
    }

    public static Class loadClass(@Nonnull String suffix, @Nonnull String key,
            @Nonnull String className, @Nonnull Class parentClass,
            @Nullable ClassLoader extraLoader) {
        if (extraLoader != null) {
            Class clazz =  loadClassHelper(suffix, key, className, parentClass, extraLoader, true);
            if (clazz != null) {
                return clazz;
            }
        }
        /**
         * A workaround the plugin framework does not work in junit
         * but we do not use the plugin framework in junit tests.
         */
        boolean junitRunning = false;
        assert (junitRunning = true);
        Class clazz = loadClassHelper(suffix, key, className, parentClass, PluginReader.class.getClassLoader(),
                junitRunning);
        return clazz;
    }

    /**
     * Reads all the the properties that match the specified suffix
     * and load them into the class map without actually loading the classes.
     * Use the {@link PluginReader#loadClass(String, String, String, Class, ClassLoader)}
     * method to load a specific class into memory.
     *
     * @param suffix
     * @param map
     */
    public static void registerLazyPlugin(@Nonnull String suffix,
            @Nonnull Map<String,String> map) {
        List<String[]> filters = PluginReader.readProperties(suffix);
        for (String[] filter : filters) {
            if (filter.length >= 2) {
                map.put(filter[0], filter[1]);
            }
        }
    }

    /**
     * Reads all the the properties that match the specified suffix
     * and load them into the map.
     *
     * @param suffix
     * @param map
     * @param parentClass
     */
    public static void registerPlugin(@Nonnull String suffix,
            @Nonnull Map<String, Class> map, @Nonnull Class parentClass) {
        List<String[]> filters = PluginReader.readProperties(suffix);
        for (String[] filter : filters) {
            if (filter.length >= 2) {
                Class clazz = loadClass(suffix, filter[0],
                        filter[1], parentClass, null);
                map.put(filter[0], clazz);
            }
        }
    }



}
