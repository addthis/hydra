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

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

/**
 * A class for managing task backups at the weekly level
 */
public class WeeklyBackup extends ScheduledBackupType {

    private final DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder().appendTwoDigitYear(2000).appendWeekOfWeekyear(2).toFormatter();

    @Override
    public String getPrefix() {
        return backupPrefix + "weekly-";
    }

    @Override
    public String getFormattedDateString(long timeMillis) {
        // For better handling of days at the end of the year.
        DateTime dt = new DateTime(timeMillis);
        if (dt.monthOfYear().get() == 12 && dt.weekOfWeekyear().get() < 3) {
            return dt.getYearOfCentury() + "53";
        }
        return Integer.toString(dt.getYearOfCentury()) + String.format("%02d", dt.weekOfWeekyear().get());
    }

    @Override
    public Date parseDateFromName(String name) throws IllegalArgumentException {
        String weekString = stripSuffixAndPrefix(name);
        if (weekString.endsWith("53")) {
            return new Date(ScheduledBackupType.getBackupCreationTimeMillis(name));
        }
        return dateTimeFormatter.parseDateTime(weekString).toDate();
    }

    @Override
    public String getDescription() {
        return "weekly";
    }
}
