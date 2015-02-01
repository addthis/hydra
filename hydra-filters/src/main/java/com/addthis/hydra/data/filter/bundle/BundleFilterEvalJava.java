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
package com.addthis.hydra.data.filter.bundle;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;

import java.io.IOException;

import java.net.MalformedURLException;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.addthis.bundle.core.Bundle;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.SuperCodable;
import com.addthis.hydra.data.compiler.JavaSimpleCompiler;
import com.addthis.hydra.data.filter.eval.InputType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link BundleFilter BundleFilter}
 * <span class="hydra-summary">evaluates a user-defined java function.</span>.
 * <p>This filter allows the user to select one or more fields from the
 * bundle for processing. These fields are copied into Java variables
 * that are processed in the user-specified function. The function must
 * return a boolean value. When the function completes the final values of the
 * Java variables are copied back into the bundle.</p>
 * <p>Unlike the value filter, the 'eval-java' bundle filter
 * uses boxed variable types ie. Longs and Doubles. The user is responsible
 * for testing for null on these variables.</p>
 * <p>If you do not wish the bundle fields to be converted from the Hydra
 * representation to the Java representation then you can use the value
 * BUNDLE_RAW in the types declaration. In this case there must be exactly
 * one type and one variable declaration (the fields parameter is ignored).
 * This is for experts only who are familiar with the internals of Hydra.</p>
 * <p>The body of the function is placed inside the {@link #body}
 * parameter. This parameter is an array of Strings for the purposes
 * of making it easier to write down multiline functions. You may choose
 * to separate each line of your function definition into a separate
 * element of the array. Or you may choose to place the function
 * body into an array of a single element.</p>
 * <p/>
 * <p>Examples:</p>
 * <pre>{op : "eval-java",
 *       body : ["c = a + b;", "return true;"],
 *       fields : ["a", "b", "c"],
 *       types : ["STRING", "STRING", "STRING"],
 *       variables : ["a", "b", "c"]
 * }</pre>
 *
 * @user-reference
 * @hydra-name eval-java
 */
public class BundleFilterEvalJava implements BundleFilter, SuperCodable {

    private static final Logger log = LoggerFactory.getLogger(BundleFilterEvalJava.class);

    /**
     * Names of the bundle fields. These
     * fields will be available in the filter
     * for processing and output. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String[] fields;

    /**
     * Java variable names that will be assigned
     * to each bundle field. Must be of equal
     * length as {@link #fields fields}.
     * This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String[] variables;

    /**
     * Java variable types that will be assigned
     * to each variable. Legal values are either primitive types, list types, or
     * map types. The primitive types are "STRING", "LONG", "DOUBLE", and "BYTES".
     * The list types are LIST_[TYPE] where [TYPE] is one of the primitive types.
     * The map types are MAP_STRING_[TYPE] where [TYPE] is one of the primitive types.
     * The bundle types are BUNDLE_RAW (see above).
     * Must be equal length as {@link #fields fields}.
     * This field is required and case sensitive.
     */
    @FieldConfig(codable = true, required = true)
    private InputType[] types;

    /**
     * Function body. This field is an array of strings. The
     * contents of the strings will be concatenated to form the
     * body of the user-defined function. The array is only
     * for the purpose of improving the user interface. You may
     * place the entire contents of the function body into
     * an array of a single element. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String[] body;

    /**
     * Optional. A set of import statements that are included
     * at the top of the generated class.
     */
    @FieldConfig(codable = true)
    private String[] imports;

    /**
     * If the input type is either a list type or a map type,
     * then this field specifies whether to process a copy of the
     * input or not. Default is true.
     */
    @FieldConfig(codable = true)
    private boolean copyInput = true;

    /**
     * This is used internally to track whether or not the user
     * is manually processing the raw bundle.
     */
    private boolean typeBundle;

    private BundleFilter constructedFilter;

    private static final Set<String> requiredImports = new HashSet<>();

    static {
        requiredImports.add("import com.addthis.bundle.core.*;");
        requiredImports.add("import com.addthis.hydra.data.filter.bundle.BundleFilter;");
        requiredImports.add("import com.addthis.hydra.data.filter.eval.*;");
        requiredImports.add("import com.addthis.bundle.value.*;");
        requiredImports.add("import com.addthis.bundle.core.*;");
        requiredImports.add("import java.util.List;");
        requiredImports.add("import java.util.Map;");
    }

    @Override
    public boolean filter(Bundle row) {
        if (constructedFilter != null) {
            return constructedFilter.filter(row);
        } else {
            return false;
        }
    }

    private BundleFilter createConstructedFilter() {
        StringBuffer classDecl = new StringBuffer();
        String classDeclString;
        UUID uuid = UUID.randomUUID();
        String className = "BundleFilter" + uuid.toString().replaceAll("-", "");
        for (String oneImport : requiredImports) {
            classDecl.append(oneImport);
            classDecl.append("\n");
        }
        if (imports != null) {
            for (String oneImport : imports) {
                if (!requiredImports.contains(oneImport)) {
                    classDecl.append(oneImport);
                    classDecl.append("\n");
                }
            }
        }
        classDecl.append("public class ");
        classDecl.append(className);
        classDecl.append(" extends BundleFilter\n");
        classDecl.append("{\n");
        createConstructor(classDecl, className);
        createFieldsVariable(classDecl);
        createInitializer(classDecl);
        createFilterMethod(classDecl);
        classDecl.append("}\n");
        classDeclString = classDecl.toString();
        JavaSimpleCompiler compiler = new JavaSimpleCompiler();

        boolean success;
        try {
            success = compiler.compile(className, classDeclString);
        } catch (IOException ex) {
            String msg = "Exception occurred while attempting to compile 'eval-java' filter.";
            msg += ex.toString();
            log.warn("Attempting to compile the following class.");
            log.warn("\n" + classDeclString);
            throw new IllegalStateException(msg);
        }

        try {
            if (!success) {
                throw handleCompilationError(classDeclString, compiler);
            }

            BundleFilter filter;
            try {
                filter = (BundleFilter) compiler.getDefaultInstance(className, BundleFilter.class);
            } catch (ClassNotFoundException | MalformedURLException |
                    InstantiationException | IllegalAccessException ex) {
                String msg =
                        "Exception occurred while attempting to classload 'eval-java' generated " +
                        "class.";
                msg += ex.toString();
                log.warn("Attempting to compile the following class.");
                log.warn("\n" + classDeclString);
                throw new IllegalStateException(msg);
            }
            return filter;
        } finally {
            compiler.cleanupFiles(className);
        }
    }

    private void createFieldsVariable(StringBuffer classDecl) {
        if (!typeBundle) {
            classDecl.append("private final String[] __fields = {");
            for (int i = 0; i < fields.length; i++) {
                classDecl.append("\"");
                classDecl.append(fields[i]);
               classDecl.append("\"");
                if (i < fields.length - 1) {
                    classDecl.append(",");
                }
            }
            classDecl.append("};\n");
        }
    }

    private void createInitializer(StringBuffer classDecl) {
        classDecl.append("public void open() {}\n");
    }

    private IllegalStateException handleCompilationError(String classDeclString, JavaSimpleCompiler compiler) {
        DiagnosticCollector<JavaFileObject> diagCollector = compiler.getDiagnostics();
        StringBuilder builder = new StringBuilder();
        builder.append("Error(s) occurred while attempting to compile 'eval-java'.\n");
        for (Diagnostic diagnostic : diagCollector.getDiagnostics()) {
            if (diagnostic.getKind().equals(Diagnostic.Kind.ERROR)) {
                builder.append(diagnostic.getMessage(null));
                builder.append(" at line ");
                builder.append(diagnostic.getLineNumber());
                builder.append(" and column ");
                builder.append(diagnostic.getColumnNumber());
                builder.append("\n");
            }
        }
        log.warn("Attempting to compile the following class.");
        log.warn("\n" + classDeclString);
        return new IllegalStateException(builder.toString());
    }

    private void createFilterMethod(StringBuffer classDecl) {
        classDecl.append("public boolean filter(Bundle __bundle)\n");
        classDecl.append("{\n");
        if (typeBundle) {
            classDecl.append("Bundle " + variables[0] + " = __bundle;\n");
        } else {
            classDecl.append("BundleField[] __bound = getBindings(__bundle, __fields);\n");

            for (int i = 0; i < variables.length; i++) {
                InputType type = types[i];
                String variable = variables[i];
                String valueVar = "__value" + i;
                String bindVar = "__bound[" + i + "]";

                classDecl.append("ValueObject ");
                classDecl.append(valueVar);
                classDecl.append(" = __bundle.getValue(");
                classDecl.append(bindVar);
                classDecl.append(");\n");
                switch (type) {
                    case LONG:
                        classDecl.append("Long");
                        break;
                   case DOUBLE:
                        classDecl.append("Double");
                        break;
                    default:
                        classDecl.append(types[i].getTypeName());
                }
                classDecl.append(" ");
                classDecl.append(variable);
                classDecl.append(" = ");
                classDecl.append(valueVar);
                classDecl.append(" == null ? null : ");
                switch (type.getCategory()) {
                  case PRIMITIVE:
                        classDecl.append(type.fromHydraAsReference(valueVar));
                        break;
                    case LIST:
                    case MAP: {
                        if (copyInput) {
                            classDecl.append(type.fromHydraAsCopy(valueVar));
                        } else {
                            classDecl.append(type.fromHydraAsReference(valueVar));
                        }
                        break;
                    }
                }
               classDecl.append(";\n");
            }
        }

        classDecl.append("try {\n");

        for (String line : body) {
            classDecl.append(line);
            classDecl.append("\n");
        }

        classDecl.append("} finally {\n");

        if (!typeBundle) {
            for (int i = 0; i < variables.length; i++) {
                InputType type = types[i];
                String variable = variables[i];
                String bindVar = "__bound[" + i + "]";
                classDecl.append("__bundle.setValue(");
                classDecl.append(bindVar);
                classDecl.append(",");
                classDecl.append(variable);
                classDecl.append(" == null ? null : ");
                classDecl.append(type.toHydra(variable));
                classDecl.append(");\n");
            }
        }

        classDecl.append("}\n");
        classDecl.append("}\n\n");
    }

    private void createConstructor(StringBuffer classDecl, String className) {
        classDecl.append("public ");
        classDecl.append(className);
        classDecl.append("() {\n");
        classDecl.append("}\n\n");
    }

    public void setBody(String[] body) {
        this.body = body;
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public void setVariables(String[] variables) {
        this.variables = variables;
    }

    public void setTypes(InputType[] types) {
        this.types = types;
    }

    @Override public void postDecode() {
        typeBundle = false;
        for (int i = 0; i < types.length; i++) {
            if (types[i].equals(InputType.BUNDLE_RAW)) {
                typeBundle = true;
            }
        }
        if (typeBundle && types.length > 1) {
            String msg = "Type 'BUNDLE_RAW' must appear entirely by itself.";
            throw new IllegalStateException(msg);
        }
        if (types.length != variables.length) {
            String msg = "Parameter 'types' and parameter 'variables' are not the same length!";
            throw new IllegalStateException(msg);
        }
        if (!typeBundle && types.length != fields.length) {
            String msg = "Parameter 'types' and parameter 'fields' are not the same length!";
            throw new IllegalStateException(msg);
        }
        constructedFilter = createConstructedFilter();
    }

    @Override public void preEncode() {}
}
