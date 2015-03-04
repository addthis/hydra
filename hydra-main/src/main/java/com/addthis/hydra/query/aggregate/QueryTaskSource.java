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

package com.addthis.hydra.query.aggregate;

import javax.annotation.Nullable;

import java.io.IOException;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;

public class QueryTaskSource {

    protected final QueryTaskSourceOption[] options;

    protected int lines = 0;
    protected long endTime = 0;

    protected TaskChannelReader dataChannelReader;

    public QueryTaskSource(QueryTaskSourceOption[] options) {
        this.options = options;
    }

    public boolean oneHasResponded() {
        return dataChannelReader != null;
    }

    public boolean complete() {
        if (options.length == 0) {
            return true;
        }
        if (endTime > 0) {
            return true;
        }
        if (oneHasResponded() && dataChannelReader.isClosed()) {
            eagerComplete();
            return true;
        }
        return false;
    }

    private void eagerComplete() {
        endTime = System.currentTimeMillis();
        // eagerly free up resources that are no longer needed -- especially any worker leases
        cancelAllActiveOptions("task is already complete");
    }

    public Bundle next() throws IOException, DataChannelError, InterruptedException {
        if (dataChannelReader == null) {
            if (!checkForReadyOption()) {
                return null;
            }
        }
        Bundle bundle = dataChannelReader.read();
        if (bundle != null) {
            lines++;
        } else if (dataChannelReader.isClosed()) {
            eagerComplete();
        }
        return bundle;
    }

    /** Whether this task has any currently active sources. This may be false either if no option was ever
     *  activated or if this task is complete. */
    public boolean hasNoActiveSources() {
        for (QueryTaskSourceOption option : options) {
            if (option.isActive()) {
                return false;
            }
        }
        return true;
    }

    public void cancelAllActiveOptions(String message) {
        for (QueryTaskSourceOption option : options) {
            if (option.isActive()) {
                option.cancel(message);
            }
        }
    }

    public QueryTaskSourceOption getSelectedSource() {
        if (oneHasResponded()) {
            return dataChannelReader.sourceOption;
        }
        return null;
    }

    public void reset() {
        cancelAllActiveOptions("resetting task source");
        lines = 0;
        dataChannelReader = null;
    }

    private void createReader(QueryTaskSourceOption readySourceOption) {
        dataChannelReader = new TaskChannelReader(readySourceOption);
    }

    @Nullable private QueryTaskSourceOption getReadyOption() {
        for (QueryTaskSourceOption option : options) {
            if (option.isReady()) {
                return option;
            }
        }
        return null;
    }

    private void cancelOtherActiveOptions(QueryTaskSourceOption selectedOption) {
        for (QueryTaskSourceOption option : options) {
            if ((option != selectedOption) && option.isActive()) {
                option.cancel("Another source option responded first");
            }
        }
    }

    private boolean checkForReadyOption() {
        QueryTaskSourceOption readyOption = getReadyOption();
        if (readyOption != null) {
            cancelOtherActiveOptions(readyOption);
            createReader(readyOption);
            return true;
        }
        return false;
    }
}
