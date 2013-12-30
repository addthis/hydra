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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * This class represents types of backups that happen on time schedules (daily, weekly, etc.) and also gold backups.
 * Its methods construct names for newly-created backups and decide whether to make new backups.
 */
public abstract class ScheduledBackupType {

    private static final Date backupsValidAfterDate = new Date(1325419200000L); // Jan 1, 2012
    private static final Map<String, ScheduledBackupType> backupTypesByDesc = ImmutableMap.of(new GoldBackup().getDescription(), new GoldBackup(), new HourlyBackup().getDescription(), new HourlyBackup(), new DailyBackup().getDescription(), new DailyBackup(), new WeeklyBackup().getDescription(), new WeeklyBackup(), new MonthlyBackup().getDescription(), new MonthlyBackup());
    private static final long goldLifeTime = Parameter.longValue("backup.gold.life.time", 90 * 60 * 1000L);
    private static final Map<ScheduledBackupType, Long> protectedBackupTypes = ImmutableMap.of((ScheduledBackupType) new GoldBackup(), goldLifeTime);
    private static final Logger log = LoggerFactory.getLogger(ScheduledBackupType.class);
    private static final Comparator<String> backupNameSorter = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            // Sort backups going from most recent to earliest
            return Long.compare(getBackupCreationTimeMillis(o2), getBackupCreationTimeMillis(o1));
        }
    };

    protected static final String backupPrefix = "b-";

    /**
     * Generates the name that should be given to a backup being made "right now".
     *
     * @param includeSuffix Whether to include the time-sensitive suffix
     * @return String name
     */
    public String generateCurrentName(boolean includeSuffix) {
        return generateNameForTime(JitterClock.globalTime(), includeSuffix);
    }

    /**
     * Generates the name that would be given to a backup made at the given time, primarily for testing.
     *
     * @param time          native time, in milliseconds
     * @param includeSuffix Whether to include the time-sensitive suffix
     * @return String name
     */
    public String generateNameForTime(long time, boolean includeSuffix) {
        return getPrefix() + getFormattedDateString(time) + (includeSuffix ? getSuffix(time) : "");
    }

    /**
     * Turn the given time into a formatted string (e.g., YYMMDD)
     *
     * @param timeMillis native time, in milliseconds
     * @return String formatted date
     */
    protected abstract String getFormattedDateString(long timeMillis);

    /**
     * Parse the date string according to the backup type's formatter
     *
     * @param name A string representing a formatted date (e.g., 120415)
     * @return Date the corresponding date
     * @throws IllegalArgumentException if the input is not valid
     */
    public abstract Date parseDateFromName(String name) throws IllegalArgumentException;

    /**
     * A static prefix that indicates the type of backup
     *
     * @return String prefix
     */
    public String getPrefix() {
        return backupPrefix;
    }

    public static String getBackupPrefix() {
        return backupPrefix;
    }

    /**
     * A dynamic suffix that should depend sensitively on the current time to avoid overwriting files
     *
     * @param time (native) time in millis
     * @return String suffix
     */
    public String getSuffix(long time) {
        return "-" + Long.toHexString(time);
    }

    /**
     * Should we create a symlink after completing the backup / what should it be called?
     * Only used for gold backups at the moment.
     *
     * @return String a non-null name if the symlink should be created; null otherwise.
     */
    public String getSymlinkName() {
        return null;
    }

    /**
     * Is this directory name a valid name for this kind of backup?
     *
     * @param name name to test
     * @return True if valid
     */
    public boolean isValidName(String name) {
        String currentDailyBackupName = generateCurrentName(true);
        if (name == null || !name.startsWith(getPrefix()) || name.length() != currentDailyBackupName.length()) {
            return false;
        }
        try {
            Date backupDate = parseDateFromName(name);
            return backupDate.after(backupsValidAfterDate) && backupDate.before(new Date(JitterClock.globalTime() + 1));
        } catch (NumberFormatException nfe) {
            return false;
        } catch (IllegalArgumentException pe) {
            return false;
        } catch (ArrayIndexOutOfBoundsException aie) {
            return false;
        }
    }

    /**
     * Strip off the prefix and suffix, leaving only the formatted-date part
     *
     * @param name input
     * @return String stripped output
     */
    public String stripSuffixAndPrefix(String name) {
        return name.substring(getPrefix().length(), name.length() - getSuffix(JitterClock.globalTime()).length());
    }

    /**
     * Should we make a new backup of this type, given that we have these backups already?
     *
     * @param existingBackups A list of directory names
     * @return True if we should backup
     */
    public boolean shouldMakeNewBackup(String[] existingBackups) {
        List<String> backupsOfThisType = getSortedListBackupsOfThisType(existingBackups);
        if (backupsOfThisType.isEmpty()) {
            return true;
        } else {
            String lastBackup = backupsOfThisType.get(backupsOfThisType.size() - 1);
            return !lastBackup.startsWith(generateCurrentName(false));
        }
    }

    /**
     * Given that certain backups exist, which backups from this type should be deleted, if any?
     *
     * @param allBackups      A list of directory names
     * @param completeBackups A list of directories that have a backup.valid file
     * @param max             The max number of backups of this type that should exist
     * @return A (possibly empty) list of backups that should be deleted.
     */
    public List<String> oldBackupsToDelete(String[] allBackups, String[] completeBackups, int max) {
        List<String> validBackupsOfThisType = getSortedListBackupsOfThisType(completeBackups);
        List<String> allBackupsOfThisType = getSortedListBackupsOfThisType(allBackups);
        List<String> rv = new ArrayList<String>();
        if (max < 0) // Indicator that we're not sure how many backups should be created
        {
            return rv;
        }
        for (String backup : allBackupsOfThisType) {
            if (!validBackupsOfThisType.contains(backup)) {
                rv.add(backup); // Delete invalid backups
            }
        }
        int excess = validBackupsOfThisType.size() - max;
        if (excess <= 0 || validBackupsOfThisType.isEmpty()) {
            return rv;
        }
        if (protectedBackupTypes.containsKey(this)) {
            String latestValidBackup = validBackupsOfThisType.get(validBackupsOfThisType.size() - 1);
            try {
                rv.addAll(validBackupsOfThisType.subList(0, excess));
            } catch (IllegalArgumentException e) {
                log.warn("Failed to parse date from backup with name " + latestValidBackup);
            }
        } else {
            rv.addAll(validBackupsOfThisType.subList(0, excess));
        }
        return rv;
    }

    /**
     * Given an array of arbitrary directory names, get the ones that are valid names of this type and sort them
     *
     * @param existingBackups input array
     * @return List all valid names
     */
    protected List<String> getSortedListBackupsOfThisType(String[] existingBackups) {
        List<String> backupsBelongingToThisType = new ArrayList<String>(existingBackups.length);
        for (String str : existingBackups) {
            if (isValidName(str)) {
                backupsBelongingToThisType.add(str);
            }
        }
        Collections.sort(backupsBelongingToThisType);
        return backupsBelongingToThisType;
    }

    /**
     * Backup types returning the same getDescription are considered equal.
     */
    @Override
    public boolean equals(Object o) {
        return o instanceof ScheduledBackupType && ((ScheduledBackupType) o).getDescription().equals(getDescription());
    }

    @Override
    public int hashCode() {
        return getDescription().hashCode();
    }

    /**
     * A description of what the type should be. Primarily used to distinguish between types.
     *
     * @return String description (e.g. "daily")
     */
    public abstract String getDescription();

    /**
     * Get an array of the registered backup types. Used to maintain parallelism between minion behavior and testing.
     *
     * @return ScheduledBackupType[] array, one of each type that is used.
     */
    public static Map<String, ScheduledBackupType> getBackupTypes() {
        return backupTypesByDesc;
    }

    /**
     * Get the backup types that need to be protected (not deleted) for a given time period
     *
     * @return The map from backup type to time period in milliseconds
     */
    public static Map<ScheduledBackupType, Long> getProtectedBackupTypes() {
        return protectedBackupTypes;
    }

    protected static long getBackupCreationTimeMillis(String name) {
        String millis = name.substring(name.lastIndexOf("-") + 1);
        try {
            return Long.parseLong(millis, 16);
        } catch (NumberFormatException nfe) {
            return -1L;
        }

    }

    @Override
    public String toString() {
        return getDescription();
    }

    public static void sortBackupsByTime(List<String> backups) {
        Collections.sort(backups, backupNameSorter);
    }
}
