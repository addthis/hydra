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
package com.addthis.hydra.task.source;

import javax.annotation.Nonnull;

import java.io.EOFException;
import java.io.IOException;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.io.DataChannelReader;
import com.addthis.codec.annotations.FieldConfig;

import com.google.common.collect.ImmutableList;

/**
 * This data source <span class="hydra-summary">accepts codec streams</span>.
 *
 * @user-reference
 * @hydra-name channel
 */
public class DataSourceChannel extends TaskDataSource implements BundleFactory {

    /**
     * This field is required.
     */
    @FieldConfig(codable = true, required = true)
    protected FactoryInputStream input;

    private final ListBundleFormat format = new ListBundleFormat();
    private DataChannelReader reader;
    private Bundle peek;

    @Override public void init() {
        try {
            reader = new DataChannelReader(this, input.createInputStream());
        } catch (IOException e) {
            throw DataChannelError.promote(e);
        }
    }

    @Override
    public Bundle next() throws DataChannelError {
        Bundle next = peek();
        peek = null;
        return next;
    }

    @Override
    public Bundle peek() throws DataChannelError {
        if (peek == null) {
            try {
                peek = reader.read();
            } catch (EOFException e) {
                return null;
            } catch (IOException e) {
                throw DataChannelError.promote(e);
            }
        }
        return peek;
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw DataChannelError.promote(e);
        }
    }

    @Override
    public Bundle createBundle() {
        return new ListBundle(format);
    }
}
