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
package com.addthis.hydra.data.filter.eval;

/**
 * Legal values are either primitive types, list types, or
 * map types. The primitive types are "STRING", "LONG", "DOUBLE", and "BYTES".
 * The list types are LIST_[TYPE] where [TYPE] is one of the primitive types.
 * The map types are MAP_[TYPE1]_[TYPE2] where [TYPE] is one of the primitive types.
 *
 * @user-reference
 */
public enum InputType {
    STRING("String", ".asString().getString()", Category.PRIMITIVE),
    LONG("long", ".asLong().getLong()", Category.PRIMITIVE),
    DOUBLE("double", ".asLong().getLong()", Category.PRIMITIVE),
    BYTES("byte[]", ".asBytes().getBytes()", Category.PRIMITIVE),
    LIST_STRING("List<String>", "ListString", Category.LIST),
    LIST_LONG("List<Long>", "ListLong", Category.LIST),
    LIST_DOUBLE("List<Double>", "ListDouble", Category.LIST),
    LIST_BYTES("List<byte[]>", "ListBytes", Category.LIST),
    MAP_STRING_STRING("Map<String,String>", "MapStringString", Category.MAP),
    MAP_STRING_LONG("Map<String,Long>", "MapStringLong", Category.MAP),
    MAP_STRING_DOUBLE("Map<String,Double>", "MapStringDouble", Category.MAP),
    MAP_STRING_BYTES("Map<String,byte[]>", "MapStringBytes", Category.MAP);

    public static enum Category {
        PRIMITIVE, LIST, MAP
    }

    private final String typeName;
    private final String wrapperName;
    private final Category category;

    InputType(String typeName, String wrapperName, Category category) {
        this.typeName = typeName;
        this.wrapperName = wrapperName;
        this.category = category;
    }

    public String getTypeName() {
        return typeName;
    }

    public String fromHydraAsReference(String variableName) {
        switch (category) {
            case PRIMITIVE:
                return variableName + wrapperName;
            case LIST:
                return "new " + wrapperName + "(" + variableName + ".asArray(), false)";
            case MAP:
                return "new " + wrapperName + "(" + variableName + ".asMap(), false)";
            default:
                return null;
        }
    }

    public String fromHydraAsCopy(String variableName) {
        switch (category) {
            case PRIMITIVE:
                return null;
            case LIST:
                return "new " + wrapperName + "(" + variableName + ".asArray(), true)";
            case MAP:
                return "new " + wrapperName + "(" + variableName + ".asMap(), true)";
            default:
                return null;
        }
    }

    public String toHydra(String variableName) {
        switch (category) {
            case PRIMITIVE:
                return "ValueFactory.create(" + variableName + ")";
            case LIST:
            case MAP:
                return "" + wrapperName + ".create(" + variableName + ")";
            default:
                return null;
        }
    }


    public Category getCategory() {
        return category;
    }
}
