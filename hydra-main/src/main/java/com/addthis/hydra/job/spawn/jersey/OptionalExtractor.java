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

import javax.ws.rs.core.MultivaluedMap;

import com.google.common.base.Optional;

import com.sun.jersey.server.impl.model.parameter.multivalued.MultivaluedParameterExtractor;

public class OptionalExtractor implements MultivaluedParameterExtractor {

    private final MultivaluedParameterExtractor extractor;

    public OptionalExtractor(MultivaluedParameterExtractor extractor) {
        this.extractor = extractor;
    }

    @Override
    public String getName() {
        return extractor.getName();
    }

    @Override
    public String getDefaultStringValue() {
        return extractor.getDefaultStringValue();
    }

    @Override
    public Object extract(MultivaluedMap<String, String> parameters) {
        return Optional.fromNullable(extractor.extract(parameters));
    }
}
