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

/**
 * This exception indicates that the problem was related to the
 * channel rather then query itself.  This is a more serious exception
 * because it impacts all queries on the channel.
 */
public class QueryChannelException extends QueryException {

    public QueryChannelException() {
        super();
    }

    public QueryChannelException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryChannelException(String message) {
        super(message);
    }

    public QueryChannelException(Throwable cause) {
        super(cause);
    }
}
