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
 * A wrapper for the queryCancelled variable so that we can pass it by reference from MQSource to the
 * query engine.
 */
public class QueryStatusObserver {

    /**
     * Stores whether or not the query related to the task currently being executed by the queryEngine has been
     * cancelled or not. The variable is volatile because it might be updated from a thread other than the
     * one in which the queryEngine is running.
     */
    public volatile boolean queryCancelled = false;

    /**
     * Stores whether or not the query has been completed. Sometimes a query would complete because there's a limit
     * involved. Objects before the limit will have no way of knowing that the query has been completed because the
     * DataChannelOuput's provide communication in one way only. This variable is one way to communicate in the
     * other direction.
     */
    public volatile boolean queryCompleted = false;
}
