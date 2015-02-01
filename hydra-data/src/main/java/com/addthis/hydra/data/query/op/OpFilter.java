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
package com.addthis.hydra.data.query.op;

import javax.annotation.Nullable;
import javax.annotation.Syntax;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.filter.bundle.BundleFilter;
import com.addthis.hydra.data.query.AbstractRowOp;

import io.netty.channel.ChannelProgressivePromise;

import static com.addthis.codec.config.Configs.decodeObject;

public class OpFilter extends AbstractRowOp {

    private final BundleFilter filter;

    public OpFilter(@Syntax("HOCON") String args, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        try {
            filter = decodeObject(BundleFilter.class, args);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Nullable @Override public Bundle rowOp(Bundle row) {
        if (filter.filter(row)) {
            return row;
        } else {
            return null;
        }
    }
}
