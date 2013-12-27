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
package com.addthis.hydra.data.query.channel;

import java.util.ArrayList;
import java.util.List;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.bundle.io.DataChannelCodec.ClassIndexMap;
import com.addthis.bundle.io.DataChannelCodec.FieldIndexMap;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.query.QueryException;

/**
 * for transport
 */
public class QueryChannelResponse implements Codec.Codable {

    @Codec.Set(codable = true)
    private List<RowByteWrapper> rowList;
    @Codec.Set(codable = true)
    private String error;
    @Codec.Set(codable = true)
    private boolean end;
    @Codec.Set(codable = true)
    private int queryId;
    @Codec.Set(codable = true)
    private boolean ping;

    // default constructor
    public QueryChannelResponse() {
    }

    public boolean isEnd() {
        return end;
    }

    public byte[] getRow(int index) {
        if (rowList != null && rowList.size() > index) {
            return rowList.get(index).getRow();
        }
        return null;
    }

    public Bundle getRow(Bundle bundle, FieldIndexMap fieldMap, ClassIndexMap classMap, int index) throws QueryException {
        if (error != null) {
            throw new QueryException(error);
        }
        try {
            byte[] row = getRow(index);
            if (row != null) {
                return DataChannelCodec.decodeBundle(bundle, row, fieldMap, classMap);
            }
            return null;
        } catch (Exception e) {
            throw new QueryException(e);
        }
    }

    public QueryChannelResponse setEnd(boolean end) {
        this.end = end;
        return this;
    }

    public QueryChannelResponse addRow(byte row[]) {
        if (this.rowList == null) {
            this.rowList = new ArrayList<RowByteWrapper>();
        }
        this.rowList.add(new RowByteWrapper(row));
        return this;
    }

    public QueryChannelResponse setError(String message) {
        this.error = message;
        this.end = true;
        return this;
    }

    public boolean isError() {
        return error != null;
    }

    public String getError() {
        return error;
    }

    public int getQueryID() {
        return queryId;
    }

    public QueryChannelResponse setQueryID(int id) {
        queryId = id;
        return this;
    }

    public QueryChannelResponse setPing(boolean ping) {
        this.ping = ping;
        return this;
    }

    public boolean isPing() {
        return ping;
    }
}
