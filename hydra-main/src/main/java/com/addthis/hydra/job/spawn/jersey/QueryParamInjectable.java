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
package com.addthis.hydra.job.spawn.jersey;

import com.sun.jersey.api.ParamException;
import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.server.impl.model.parameter.multivalued.ExtractorContainerException;
import com.sun.jersey.server.impl.model.parameter.multivalued.MultivaluedParameterExtractor;

public class QueryParamInjectable extends AbstractHttpContextInjectable<Object> {

    private final MultivaluedParameterExtractor extractor;
    private final boolean decode;

    public QueryParamInjectable(MultivaluedParameterExtractor extractor,
            boolean decode) {
        this.extractor = extractor;
        this.decode = decode;
    }

    @Override
    public Object getValue(HttpContext c) {
        try {
            return extractor.extract(c.getUriInfo().getQueryParameters(decode));
        } catch (ExtractorContainerException e) {
            throw new ParamException.QueryParamException(e.getCause(),
                    extractor.getName(),
                    extractor.getDefaultStringValue());
        }
    }
}
