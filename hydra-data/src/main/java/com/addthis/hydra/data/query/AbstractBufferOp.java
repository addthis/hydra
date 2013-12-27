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

import java.util.ArrayList;
import java.util.List;

import com.addthis.bundle.core.Bundle;

/**
 * this class of operator should may drop or create lines
 * using the existing rows as sources.  it could alternatively
 * act like a table op and consume the previous stream emitting
 * it's own.
 */
public abstract class AbstractBufferOp extends AbstractQueryOp {

    /**
     * TODO
     */
    public abstract List<Bundle> finish();

    /**
     * TODO
     */
    public abstract List<Bundle> next(Bundle row);

    /**
     * helper to return a single row
     */
    public List<Bundle> createRows(Bundle row, int count) {
        ArrayList<Bundle> single = new ArrayList<Bundle>(count);
        single.add(row);
        return single;
    }

    public List<Bundle> createRows(Bundle row) {
        return createRows(row, 1);
    }

    @Override
    public void send(List<Bundle> rows) {
        getNext().send(rows);
    }

    @Override
    public void send(Bundle row) {
        send(next(row));
    }

    @Override
    public void sendComplete() {
        send(finish());
        getNext().sendComplete();
    }
}
