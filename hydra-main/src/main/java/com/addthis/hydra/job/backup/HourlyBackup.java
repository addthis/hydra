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
package com.addthis.hydra.job.backup;

import java.util.Date;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

/**
 * A class for managing task backups at the hourly level
 */
public class HourlyBackup extends ScheduledBackupType {

    private final DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder().appendTwoDigitYear(2000).appendMonthOfYear(2).appendDayOfMonth(2).appendLiteral("-").appendHourOfDay(2).toFormatter();

    @Override
    public String getFormattedDateString(long timeMillis) {
        return dateTimeFormatter.print(timeMillis);
    }

    @Override
    public Date parseDateFromName(String name) throws IllegalArgumentException {
        return dateTimeFormatter.parseDateTime(stripSuffixAndPrefix(name)).toDate();
    }

    @Override
    public String getPrefix() {
        return backupPrefix + "hourly-";
    }

    @Override
    public String getDescription() {
        return "hourly";
    }
}
