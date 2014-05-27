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

import java.io.IOException;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.query.FramedDataChannelReader;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.stream.SourceInputStream;

public class QueryTaskSource {

    protected final QueryTaskSourceOption[] options;

    protected int lines = 0;
    protected long endTime = 0;

    protected FramedDataChannelReader dataChannelReader;

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
        if (oneHasResponded() && dataChannelReader.eof.get()) {
            endTime = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    public Bundle next() throws IOException, DataChannelError {
        if (dataChannelReader == null) {
            if (!checkForReadyOption()) {
                return null;
            }
        }
        Bundle bundle = dataChannelReader.read();
        if (bundle != null) {
            lines++;
        }
        return bundle;
    }

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
            for (QueryTaskSourceOption option : options) {
                if (option.isActive()) {
                    return option;
                }
            }
        }
        return null;
    }

    public void reset() {
        cancelAllActiveOptions("resetting task source");
        lines = 0;
        dataChannelReader = null;
    }

    private void createReader(SourceInputStream sourceInputStream, FileReference queryReference) {
        dataChannelReader = new FramedDataChannelReader(
                sourceInputStream, queryReference.name, AggregateConfig.FRAME_READER_POLL);
    }

    private QueryTaskSourceOption getReadyOption() {
        for (QueryTaskSourceOption option : options) {
            if (option.isActive() && (option.sourceInputStream.available() > 0)) {
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
            createReader(readyOption.sourceInputStream, readyOption.queryReference);
            return true;
        }
        return false;
    }
}
