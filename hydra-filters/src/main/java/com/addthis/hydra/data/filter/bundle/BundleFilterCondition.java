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

import javax.validation.constraints.NotNull;

import com.addthis.bundle.core.Bundle;
import com.addthis.codec.annotations.FieldConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">conditionally executes a bundle filter</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {op:"condition",
 *      ifCondition: {op:"contains", field:"SRC", value:["foo", "bar"]},
 *      ifDo: {op:"clear", field:"SRC"}},
 * </pre>
 *
 * @user-reference
 * @hydra-name condition
 */
public class BundleFilterCondition extends BundleFilter {

    /** The conditional bundle filter. This field is required. */
    @NotNull
    @FieldConfig(codable = true)
    BundleFilter ifCondition;

    /**
     * The bundle filter to execute when {@link #ifCondition ifCondition} returns true. This
     * field is required.
     */
    @FieldConfig(codable = true)
    BundleFilter ifDo;

    /**
     * The bundle filter to execute when {@link #ifCondition ifCondition} returns false. This
     * field is optional.
     */
    @FieldConfig(codable = true)
    BundleFilter elseDo;

    /**
     * If true then return the result of the {@link #ifDo ifDo} or the {@link #elseDo elseDo}
     * whichever branch is executed. This field is optional. Default false.
     */
    @FieldConfig(codable = true)
    boolean returnFilter;

    // for CodecJson
    public BundleFilterCondition() {}

    public BundleFilterCondition(@JsonProperty("if")   BundleFilter ifCondition,
                                 @JsonProperty("then") BundleFilter ifDo,
                                 @JsonProperty("else") BundleFilter elseDo) {
        this.ifCondition = ifCondition;
        this.ifDo = ifDo;
        this.elseDo = elseDo;
    }

    @Override
    public void initialize() {
        ifCondition.initOnceOnly();
        if (ifDo != null) {
            ifDo.initOnceOnly();
        }
        if (elseDo != null) {
            elseDo.initOnceOnly();
        }
    }

    @Override
    public boolean filterExec(Bundle row) {
        boolean returnValue = true;
        if (row != null) {
            if (ifCondition != null && ifCondition.filterExec(row)) {
                if (ifDo != null) {
                    boolean result = ifDo.filterExec(row);
                    returnValue = returnFilter ? result : returnValue;
                }
            } else {
                if (elseDo != null) {
                    boolean result = elseDo.filterExec(row);
                    returnValue = returnFilter ? result : returnValue;
                }
            }
        }
        return returnValue;
    }
}
