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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractQueryOp;


/**
 * <p>This query operation <span class="hydra-summary">inserts a row at the top of the data</span>.
 * <p>The syntax for this operation is title=foo,bar,baz,etc.</p>
 *
 * @user-reference
 * @hydra-name title
 */
public class OpTitle extends AbstractQueryOp {

    private final AtomicBoolean sentTitle = new AtomicBoolean(false);
    private final LinkedList<ValueObject> header = new LinkedList<>();

    /**
     * @param args
     */
    public OpTitle(String args) {
        this(Strings.splitArray(args, ","));
    }

    /**
     * @param title
     */
    public OpTitle(String title[]) {
        for (String col : title) {
            header.add(ValueFactory.create(col));
        }
    }

    @Override
    public void send(Bundle row) throws DataChannelError {
        if (sentTitle.compareAndSet(false, true)) {
            Bundle title = row.createBundle();
            if (header.size() > 0) {
                for (Iterator<ValueObject> iter = header.iterator(); iter.hasNext();) {
                    getSourceColumnBinder(row).appendColumn(title, iter.next());
                }
            } else {
                for (BundleField field : row) {
                    title.setValue(field, ValueFactory.create(field.getName()));
                }
            }
            getNext().send(title);
        }
        getNext().send(row);
    }

    @Override
    public void sendComplete() {
        getNext().sendComplete();
    }
}
