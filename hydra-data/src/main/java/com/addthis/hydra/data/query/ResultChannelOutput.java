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
package com.addthis.hydra.data.query;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;

import io.netty.channel.ChannelProgressivePromise;


public class ResultChannelOutput extends AbstractQueryOp {

    private final DataChannelOutput output;

    private int lines = 0;

    public ResultChannelOutput(DataChannelOutput output, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        this.output = output;
    }

    public DataChannelOutput getOutput() {
        return output;
    }

    private void reportLines() {
        opPromise.tryProgress(0, lines);
        lines = 0;
    }

    @Override
    public void send(Bundle row) throws DataChannelError {
        output.send(row);
        lines += 1;
        if (lines >= 100) {
            reportLines();
        }
    }

    @Override
    public void sendComplete() {
        reportLines();
        if (opPromise.cause() != null) {
            output.sourceError(promoteHackForThrowables(opPromise.cause()));
        } else {
            output.sendComplete();
        }
    }

    @Override
    public String getSimpleName() {
        return "ResultChannelOutput(" + output + ")";
    }

    private static DataChannelError promoteHackForThrowables(Throwable cause) {
        if (cause instanceof DataChannelError) {
            return (DataChannelError) cause;
        } else {
            return new DataChannelError(cause);
        }
    }
}
