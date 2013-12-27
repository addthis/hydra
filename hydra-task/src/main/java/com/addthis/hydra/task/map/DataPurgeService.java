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
package com.addthis.hydra.task.map;

import org.joda.time.DateTime;

/**
 * A service for deleting data from local data stores.  The service
 * expects the invoker to provide a template describing how the data
 * is stored by date.  The service is designed to purge data that is
 * stored in dated director structures, e.g.
 * <p/>
 * directoryPrefix/11/01/02
 * <p/>
 * <p/>
 * The purge class does what it says so it should be used with caution.
 * Once the method is invoked the data will be deleted, no local backup
 * is created for the data.
 */
public interface DataPurgeService {

    /**
     * Purges data based on the current time and the maximum age the caller specified.  For example
     * if the <code>maxAgeInDays</code> field is 10, then any data older than 10 days (exclusive)
     * will be purged from the local file system.
     *
     * @param dataPurgeConfig - the data purge configuration used by the purge service
     * @param currentTime     - the current time used as reference for data purging
     * @return - true if successful
     */
    boolean purgeData(DataPurgeConfig dataPurgeConfig, DateTime currentTime);
}
