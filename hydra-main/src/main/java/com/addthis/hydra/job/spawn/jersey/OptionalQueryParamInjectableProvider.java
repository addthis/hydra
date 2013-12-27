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

import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import com.google.common.base.Optional;

import com.sun.jersey.api.model.Parameter;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.core.spi.component.ProviderServices;
import com.sun.jersey.server.impl.model.parameter.multivalued.MultivaluedParameterExtractorFactory;
import com.sun.jersey.server.impl.model.parameter.multivalued.StringReaderFactory;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;

/**
 * https://raw.github.com/codahale/dropwizard/master/dropwizard-core/src/main/java/com/yammer/dropwizard/jersey/OptionalQueryParamInjectableProvider.java
 * Licensed under the Apache License https://github.com/dropwizard/dropwizard/blob/master/LICENSE
 * Copyright 2010-2013 Coda Hale and Yammer, Inc.
 */
@Provider
public class OptionalQueryParamInjectableProvider implements InjectableProvider<QueryParam, Parameter> {

    private MultivaluedParameterExtractorFactory factory;
    private ProviderServices services;

    public OptionalQueryParamInjectableProvider(@Context ProviderServices services) {
        this.services = services;
    }

    @Override
    public ComponentScope getScope() {
        return ComponentScope.PerRequest;
    }

    @Override
    public Injectable<?> getInjectable(ComponentContext ic,
            QueryParam a,
            Parameter c) {
        if (isExtractable(c)) {
            final OptionalExtractor extractor = new OptionalExtractor(getFactory().get(unpack(c)));
            return new QueryParamInjectable(extractor, !c.isEncoded());
        }
        return null;
    }

    private boolean isExtractable(Parameter param) {
        return (param.getSourceName() != null) && !param.getSourceName().isEmpty() &&
               param.getParameterClass().isAssignableFrom(Optional.class) &&
               (param.getParameterType() instanceof ParameterizedType);
    }

    private Parameter unpack(Parameter param) {
        final Type typeParameter = ((ParameterizedType) param.getParameterType()).getActualTypeArguments()[0];
        return new Parameter(param.getAnnotations(),
                param.getAnnotation(),
                param.getSource(),
                param.getSourceName(),
                typeParameter,
                (Class<?>) typeParameter,
                param.isEncoded(),
                param.getDefaultValue());
    }

    private MultivaluedParameterExtractorFactory getFactory() {
        if (factory == null) {
            final StringReaderFactory stringReaderFactory = new StringReaderFactory();
            stringReaderFactory.init(services);

            this.factory = new MultivaluedParameterExtractorFactory(stringReaderFactory);
        }
        return factory;
    }
}
