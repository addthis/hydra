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
package com.addthis.hydra.task.map;

import com.addthis.hydra.data.filter.bundle.BundleFilter;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This section defines the transformations to apply onto the data.
 * <p/>
 * <p>The {@link #fields fields} section defines how the fields of the input source
 * are transformed into a mapped bundle. The {@link #filterIn filterIn} filter is applied
 * before the fields transformation. The {@link #filterOut filterOut} filter is applied
 * after the fields transformation. filterIn can be used to improve job performance
 * by eliminating unneeded records so that they do not need to be transformed.</p>
 * <p/>
 * <p>To specify a series of filters for the filterIn or filterOut use
 * a {@link com.addthis.hydra.data.filter.bundle.BundleFilterChain chain} bundle filter.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>map:{
 *    filterIn: {op:"chain", filter:[
 *       {op:"field", from:"TIME", filter:{op:"chain", filter:[
 *          {op:"empty", not:true},
 *          {op:"require", match:["[0-9]{13}"]},
 *       ]}},
 *    ]},
 *    fields:[
 *       {from:"TIME", to:"TIME"},
 *       {from:"SOURCE", to:"SOURCE"},
 *       {from:"QUERY_PARAMS"},
 *    ],
 *    filterOut:{op:"chain", filter:[
 *       {op:"time", src:{field:"TIME", format:"native"},
 *             dst:{field:"DATE", format:"yyMMdd-HHmmss", timeZone:"America/New_York"}},
 *       {op:"field", from:"DATE", to:"DATE_YMD", filter:{op:"slice", to:6}},
 *    ]}
 * }</pre>
 *
 * @user-reference
 */
public final class MapDef {

    /** The filter to apply before field transformation. */
    @JsonProperty BundleFilter filterIn;

    /** The filter to apply after field transformation. */
    @JsonProperty BundleFilter filterOut;

    /** The mapping of fields from the input source into the bundle. */
    @JsonProperty FieldFilter[] fields;

    public void init() {
        if (filterIn != null) {
            filterIn.open();
        }
        if (filterOut != null) {
            filterOut.open();
        }
    }
}
