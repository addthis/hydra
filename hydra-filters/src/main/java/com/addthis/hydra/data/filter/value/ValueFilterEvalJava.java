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
package com.addthis.hydra.data.filter.value;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;

import java.io.IOException;

import java.net.MalformedURLException;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.SuperCodable;
import com.addthis.hydra.data.compiler.JavaSimpleCompiler;
import com.addthis.hydra.data.filter.eval.InputType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">evaluates
 * a user-defined java function.</span>.
 * <p/>
 * <p>The user-defined java function is expected to operate on either
 * primitive values (long, double, String, byte[]) or a list
 * or a map over autoboxed primitive types (Long, Double, String, byte[]).
 * If the input is a list or a map then remember to set the {@link #once once}
 * parameter to 'true'. The default type of the input variable is a long.
 * <p/>
 * <p>If the input passed into the filter is null and the input type
 * is LONG or DOUBLE then the filter always returns null. If the input
 * is null and the input type is neither LONG nor DOUBLE then the user
 * is responsible for handling the null input.
 * <p/>
 * <p>By default the name of the input variable is "x".
 * This name can be changed with the parameter {@link #inputName}.
 * The type of the input variable is specified by the parameter
 * {@link #inputType}.</p>
 * <p/>
 * <p>The body of the function is placed inside the {@link #body}
 * parameter. This parameter is an array of Strings for the purposes
 * of making it easier to write down multiline functions. You may choose
 * to separate each line of your function definition into a separate
 * element of the array. Or you may choose to place the function
 * body into an array of a single element.</p>
 * <p/>
 * <p>Examples:</p>
 * <pre>
 *   // switch statements cannot operate on long types.
 *   // so convert the input to an integer type.
 *   { op : "eval-java", inputName: "longInput", inputType: "LONG",
 *     outputType: "STRING",
 *     body : [
 *       "int intInput = (int) longInput;" ,
 *       "switch(intInput)",
 *       "{",
 *       "case 1: return \"foo\";",
 *       "case 2: return \"bar\";",
 *       "default: return \"baz\";",
 *       "}"
 *     ]
 *   }
 *
 *  { op : "eval-java", inputName: "input", inputType: "STRING",
 *    outputType: "STRING", body : ["return String.format(\"Hello %s\", input);"]
 *  }
 *
 *  { op : "eval-java", inputName: "input", inputType: "LIST_STRING",
 *    outputType: "STRING", once : true, body : [
 *      "if (input.size() < 3),
 *      "return null;"
 *      "else return input.get(2);"
 *    ]
 *  }</pre>
 *
 * @user-reference
 * @hydra-name eval-java
 */
public class ValueFilterEvalJava extends AbstractValueFilter implements SuperCodable {

    private static final Logger log = LoggerFactory.getLogger(ValueFilterEvalJava.class);

    /**
     * Name of the input variable. The name
     * "value" is not allowed. Default is "x".
     */
    @FieldConfig(codable = true)
    private String inputName = "x";

    /**
     * Type of the input variable. Legal values are either primitive types, list types, or
     * map types. The primitive types are "STRING", "LONG", "DOUBLE", and "BYTES".
     * The list types are LIST_[TYPE] where [TYPE] is one of the primitive types.
     * The map types are MAP_STRING_[TYPE] where [TYPE] is one of the primitive types.
     * This field is case sensitive. Default is "LONG".
     */
    @FieldConfig(codable = true)
    private InputType inputType = InputType.LONG;

    /**
     * Type of the return variable. Legal values are either primitive types, list types, or
     * map types. The primitive types are "STRING", "LONG", "DOUBLE", and "BYTES".
     * The list types are LIST_[TYPE] where [TYPE] is one of the primitive types.
     * The map types are MAP_STRING_[TYPE] where [TYPE] is one of the primitive types.
     * This field is case sensitive. Default is "LONG".
     */
    @FieldConfig(codable = true)
    private InputType outputType = InputType.LONG;

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
    private boolean copyInput = true;

    private ValueFilter constructedFilter;

    private ValueFilterEvalJava() {}

    private static final Set<String> requiredImports = new HashSet<>();

    static {
        requiredImports.add("import com.addthis.hydra.data.filter.eval.*;");
        requiredImports.add("import com.addthis.hydra.data.filter.value.ValueFilter;");
        requiredImports.add("import com.addthis.hydra.data.filter.value.AbstractValueFilter;");
        requiredImports.add("import com.addthis.bundle.value.DefaultArray;");
        requiredImports.add("import com.addthis.bundle.value.ValueArray;");
        requiredImports.add("import com.addthis.bundle.value.ValueObject;");
        requiredImports.add("import com.addthis.bundle.value.ValueFactory;");
        requiredImports.add("import java.util.List;");
        requiredImports.add("import java.util.Map;");
    }

    @Override public void postDecode() {
        constructedFilter = createConstructedFilter();
    }

    @Override public void preEncode() {}

    @Override
    public ValueObject filterValue(ValueObject value) {
        if (constructedFilter != null) {
            return constructedFilter.filter(value);
        } else {
            return null;
        }
    }

    private ValueFilter createConstructedFilter() {
        StringBuffer classDecl = new StringBuffer();
        String classDeclString;
        UUID uuid = UUID.randomUUID();
        String className = "ValueFilter" + uuid.toString().replaceAll("-", "");
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
        classDecl.append(" extends AbstractValueFilter\n");
        classDecl.append("{\n");
        createConstructor(classDecl, className);
        createInitializer(classDecl);
        createFilterValueMethod(classDecl);
        createFilterValueInternalMethod(classDecl);
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

            ValueFilter filter;
            try {
                filter = (ValueFilter) compiler.getDefaultInstance(className, AbstractValueFilter.class);
            } catch (ClassNotFoundException | MalformedURLException |
                    InstantiationException | IllegalAccessException ex) {
                String msg = "Exception occurred while attempting to classload 'eval-java' generated class.";
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

    private void createFilterValueInternalMethod(StringBuffer classDecl) {
        classDecl.append("private ");
        classDecl.append(outputType.getTypeName());
        classDecl.append(" filterValueInternal(");
        classDecl.append(inputType.getTypeName());
        classDecl.append(" ");
        classDecl.append(inputName);
        classDecl.append(")\n");
        classDecl.append("{\n");
        for (String line : body) {
            classDecl.append(line);
            classDecl.append("\n");
        }
        classDecl.append("}\n\n");
    }

    private void createInitializer(StringBuffer classDecl) {
        classDecl.append("public void open() {}\n");
    }

    private void createFilterValueMethod(StringBuffer classDecl) {
        classDecl.append("public ValueObject filterValue(ValueObject value)\n");
        classDecl.append("{\n");
        if (inputType == InputType.DOUBLE || inputType == InputType.LONG) {
            classDecl.append("if (value == null)\n");
            classDecl.append("return null;\n");
            classDecl.append(inputType.getTypeName());
            classDecl.append(" input = ");
        } else {
            classDecl.append(inputType.getTypeName());
            classDecl.append(" input = (value == null) ? null : ");
        }
        switch (inputType.getCategory()) {
            case PRIMITIVE:
                classDecl.append(inputType.fromHydraAsReference("value"));
                break;
            case LIST:
            case MAP: {
                if (copyInput) {
                    classDecl.append(inputType.fromHydraAsCopy("value"));
                } else {
                    classDecl.append(inputType.fromHydraAsReference("value"));
                }
                break;
            }
        }
        classDecl.append(";\n");
        classDecl.append(outputType.getTypeName());
        classDecl.append(" output = filterValueInternal(input);\n");
        classDecl.append("return ");
        classDecl.append(outputType.toHydra("output"));
        classDecl.append(";\n");
        classDecl.append("}\n\n");
    }

    private void createConstructor(StringBuffer classDecl, String className) {
        classDecl.append("public ");
        classDecl.append(className);
        classDecl.append("() {\n");
        classDecl.append("setOnce(");
        classDecl.append(getOnce());
        classDecl.append(");\n");
        classDecl.append("}\n\n");
    }

    public void setInputType(InputType type) {
        this.inputType = type;
    }

    public void setBody(String[] body) {
        this.body = body;
    }

    public void setCopyInput(boolean val) {
        this.copyInput = val;
    }

    public void setOutputType(InputType type) {
        this.outputType = type;
    }
}
