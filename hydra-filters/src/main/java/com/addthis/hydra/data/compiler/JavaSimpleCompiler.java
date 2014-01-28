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
package com.addthis.hydra.data.compiler;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaSimpleCompiler {

    private static Logger log = LoggerFactory.getLogger(JavaSimpleCompiler.class);
    private final StandardJavaFileManager fileManager;
    private final JavaCompiler compiler;
    DiagnosticCollector<JavaFileObject> diagnostics;

    public JavaSimpleCompiler() {
        compiler = ToolProvider.getSystemJavaCompiler();
        fileManager = compiler.getStandardFileManager(null, null, null);
        diagnostics = new DiagnosticCollector<>();
    }

    public boolean compile(String className, String body) throws IOException {
        File sourceFile = new File("/tmp/" + className + ".java");

        BufferedWriter out = new BufferedWriter(new FileWriter(sourceFile));
        out.write(body);
        out.close();
        List<String> optionList = new ArrayList<>();
        String classpath = System.getProperty("java.class.path");
        log.info("Classpath is " + classpath);
        optionList.addAll(Arrays.asList("-cp", classpath));
        JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
        Iterable<? extends JavaFileObject> compilationUnits =
                fileManager.getJavaFileObjects(sourceFile);
        boolean success = javaCompiler.getTask(null, fileManager, diagnostics,
                optionList, null, compilationUnits).call();

        return success;
    }

    public Object getDefaultInstance(String className, Class<?> parentClass)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException,
                   MalformedURLException {
        File classpath = new File("/tmp");
        URL[] urlPath = {classpath.toURI().toURL()};
        URLClassLoader loader;
        if (parentClass != null) {
            loader = new URLClassLoader(urlPath, parentClass.getClassLoader());
        } else {
            loader = new URLClassLoader(urlPath);
        }
        return (loader.loadClass(className).newInstance());
    }

    public void cleanupFiles(String className) {
        File sourceFile = new File("/tmp/" + className + ".java");
        File classFile = new File("/tmp/" + className + ".class");
        sourceFile.delete();
        classFile.delete();
    }

    public DiagnosticCollector<JavaFileObject> getDiagnostics() {
        return diagnostics;
    }

}
