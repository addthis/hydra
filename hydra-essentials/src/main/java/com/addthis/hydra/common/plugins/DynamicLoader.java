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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

public class DynamicLoader {

    private static final Logger log = LoggerFactory.getLogger(DynamicLoader.class);

    private static final CsvPreference csvParserOptions =
            new CsvPreference.Builder(CsvPreference.STANDARD_PREFERENCE)
                    .surroundingSpacesNeedQuotes(true)
                    .build();

    public static class DynamicLoaderResult {
        public Map<String,String> executables;
        public ClassLoader loader;

        public DynamicLoaderResult() {
            executables = Collections.unmodifiableMap(new HashMap<String,String>());
            loader = null;
        }
    }

    /**
     * Looks up the system property specified as input.
     * Treats the value associated with the input as a URI.
     * The contents of the URI should be a CSV file.
     * Each row of the the file should have at least one column.
     * The first column is the fully qualified class name of
     * a class to load. The second column specifies a URI which
     * the classloader will use to search for the class.
     * A legal value for the second column is the empty string ""
     * if you don't want to copy/paste the URI to multiple rows.
     * Executable targets should be specified with four columns:
     * [fully qualified class name], [uri], "executable", [name]
     * The fourth column is the execution name to be associated with
     * the class.
     *
     * @param propertyName system property associated with a URI
     * @return map of execution target names to fully qualified class names
     */
    public static DynamicLoaderResult readDynamicClasses(@Nonnull String propertyName) {
        Map<String,String> executables = new HashMap<>();
        DynamicLoaderResult result = new DynamicLoaderResult();
        String urlSpecifier = System.getProperty(propertyName);
        if (urlSpecifier == null) {
            return result;
        }
        URL url;
        try {
            url = new URL(urlSpecifier);
        } catch (MalformedURLException ex) {
            log.error("The value for the system property {} " +
                      "is \"{}\" cannot be translated into a valid URL.",
                    propertyName, urlSpecifier);
            return result;
        }
        Set<URL> jarLocations = new HashSet<>();
        Set<String> classNames = new HashSet<>();
        readDynamicClasses(urlSpecifier, url, jarLocations, classNames, executables);
        ClassLoader loader = loadDynamicClasses(jarLocations, classNames);
        result.executables = Collections.unmodifiableMap(executables);
        result.loader = loader;
        return result;
    }

    private static URLClassLoader loadDynamicClasses(Set<URL> jarLocations,
            Set<String> classNames) {
        String jarString = Joiner.on(", ").join(jarLocations);
        URLClassLoader loader = new URLClassLoader(
                jarLocations.toArray(new URL[jarLocations.size()]));
        for(String className : classNames) {
             try {
                loader.loadClass(className);
             } catch (ClassNotFoundException ex) {
                log.error("ClassNotFoundException when attempting" +
                          "to load {} from classpath {} and {}",
                          className, jarString, System.getProperty("java.class.path"));
            }
        }
        return loader;
    }

    private static void readDynamicClasses(String urlSpecifier,
            URL url, Set<URL> jarLocations,
            Set<String> classNames, Map<String,String> executables) {
        InputStreamReader reader = null;
        try {
            InputStream stream = url.openStream();
            reader = new InputStreamReader(stream);
            CsvListReader csvParser = new CsvListReader(reader, csvParserOptions);
            List<String> next;
            int line = 0;
            while ((next = csvParser.read()) != null) {
                line++;
                String[] fields = next.toArray(new String[next.size()]);
                try {
                    if (fields.length < 1) {
                        log.error("Line {} in the file {} contains less than one column",
                                line, urlSpecifier);
                    } else {
                        classNames.add(fields[0]);
                        if (fields.length > 1 && fields[1].length() > 0) {
                            jarLocations.add(new URL(fields[1]));
                        }
                    }
                    if (fields.length >= 4) {
                        if (fields[2].equals("executable")) {
                            executables.put(fields[3], fields[0]);
                        }
                    }
                } catch (MalformedURLException ex) {
                    if (fields.length >= 2) {
                        log.error("Could not parse url \"{}\" from {}",
                                fields[1], urlSpecifier);
                    } else {
                        log.error("Unexpected error in {}: {}",
                                urlSpecifier, ex);
                    }
                }
            }
        } catch (IOException ex) {
            log.error("Error attempting to read from {}: {}", urlSpecifier, ex);
        } catch (SuperCsvException ex) {
            log.error("CSV parse error in {}: {}", urlSpecifier, ex);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch(IOException ex) {
                    log.error("Error closing stream: ", ex);
                }
            }
        }
    }


}
