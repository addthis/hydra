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
package com.addthis.hydra.job;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.addthis.basis.util.JitterClock;

import com.addthis.hydra.job.backup.DailyBackup;
import com.addthis.hydra.job.backup.GoldBackup;
import com.addthis.hydra.job.backup.HourlyBackup;
import com.addthis.hydra.job.backup.ScheduledBackupType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ScheduledBackupTypeTest {

    private static final Collection<ScheduledBackupType> testedTypes = ScheduledBackupType.getBackupTypes().values();
    private static final Date backupsValidStartDate = new Date(1325419200000l); // Jan 1, 2012
    private static final long now = JitterClock.globalTime();

    private static long daysAgo(int numDays) {
        return now - numDays * 86400000l;
    }

    @Test
    public void shouldMakeNewBackupTest() throws Exception {
        // For each backup type, ensure that a new backup will _not_ be made if there is a current backup available
        // Also ensure that a new backup _will_ be made if the existing backup is old / doesn't exist
        for (ScheduledBackupType type : testedTypes) {
            String currBackupName = type.generateCurrentName(true);
            String oldBackupName = type.generateNameForTime(daysAgo(60), true);
            String[] oldAndNewBackup = new String[]{oldBackupName, currBackupName};
            String[] anOldBackup = new String[]{oldBackupName};
            String desc = type.getDescription();
            boolean isGold = type instanceof GoldBackup; // Gold is a special case; we always make gold backups
            assertEquals("shouldn't make a " + desc + " backup if a current one exists, unless gold",
                    isGold, type.shouldMakeNewBackup(oldAndNewBackup));
            assertEquals("should make a " + desc + " backup if other backup is old", true, type.shouldMakeNewBackup(anOldBackup));
            assertEquals("should make a " + desc + " backup if no backups", true, type.shouldMakeNewBackup(new String[]{}));
        }
    }

    @Test
    public void oldBackupsToDeleteTest() throws Exception {
        // For each backup type, suppose we have 4 backups from that type and some other directories mixed in.
        // Then if our max number of backups is n, ensure 4-n of the oldest backups are deleted.
        // Also make sure we don't touch any of the other directories.
        for (ScheduledBackupType type : testedTypes) {
            String currBackupName = type.generateCurrentName(true);
            String monthOldBackupName = type.generateNameForTime(daysAgo(31), true);
            String twoMonthOldBackupName = type.generateNameForTime(daysAgo(62), true);
            String threeMonthOldBackupName = type.generateNameForTime(daysAgo(93), true);
            String[] existingBackups = new String[]{currBackupName, monthOldBackupName, twoMonthOldBackupName,
                                                    threeMonthOldBackupName, "live", "replica", "otherdirectory"};
            String desc = type.getDescription();
            for (int max = 1; max < 4; max++) {
                List<String> backupsToDelete = type.oldBackupsToDelete(existingBackups, existingBackups, max);
                assertEquals("should delete all but " + max + " backups for type " + desc, 4 - max, backupsToDelete.size());
                for (String backupToDelete : backupsToDelete) {
                    assertTrue("should delete a backup from type " + desc, type.isValidName(backupToDelete));
                    assertTrue("shouldn't delete the current backup for type " + desc, !backupToDelete.equals(currBackupName));
                }
            }
        }
    }

    @Test
    public void invalidBackupsDeleteTest() throws Exception {
        // For each backup type, suppose we have four valid backups from that type and one invalid backup
        // If the max number of backups for this type is two, we should delete the invalid backup and also the two oldest valid backups
        for (ScheduledBackupType type : testedTypes) {
            String currBackupName = type.generateCurrentName(true);
            String invalidBackupName = type.generateNameForTime(daysAgo(31), true);
            String twoMonthOldBackupName = type.generateNameForTime(daysAgo(62), true);
            String threeMonthOldBackupName = type.generateNameForTime(daysAgo(93), true);
            String fourMonthOldBackupName = type.generateNameForTime(daysAgo(124), true);
            String[] allBackups = new String[]{currBackupName, invalidBackupName, twoMonthOldBackupName,
                                               threeMonthOldBackupName, fourMonthOldBackupName, "live", "replica", "otherdirectory"};
            String[] validBackups = new String[]{currBackupName, twoMonthOldBackupName,
                                                 threeMonthOldBackupName, fourMonthOldBackupName, "live", "replica", "otherdirectory"};
            List<String> backupsToDelete = type.oldBackupsToDelete(allBackups, validBackups, 2);
            String desc = type.getDescription();
            assertEquals("should delete three old backups", 3, backupsToDelete.size());
            for (String backupToDelete : backupsToDelete) {
                assertTrue("should delete a backup from type " + desc, type.isValidName(backupToDelete));
                assertTrue("should delete an invalid backup or one of the two oldest backups",
                        backupToDelete.equals(invalidBackupName) || backupToDelete.equals(threeMonthOldBackupName) || backupToDelete.equals(fourMonthOldBackupName));
            }
        }
    }

    @Test
    public void parsingTest() throws Exception {
        // Make sure that each backup type can recognize and parse dates from its own generated names
        for (ScheduledBackupType type : testedTypes) {
            String desc = type.getDescription();
            String currBackup = type.generateCurrentName(true);
            assertEquals("current " + desc + " backup should be valid name", true, type.isValidName(currBackup));
            Date date = type.parseDateFromName(currBackup);
            assertEquals("parsed " + desc + " backup date should be after start date", true, date.after(backupsValidStartDate));
        }
    }

    @Test
    public void noCollisionsTest() throws Exception {
        // Make sure all backup names are valid for their originating types and invalid for all other types
        for (ScheduledBackupType type1 : testedTypes) {
            String currBackupName = type1.generateCurrentName(true);
            for (ScheduledBackupType type2 : testedTypes) {
                if (type1.getDescription().equals(type2.getDescription())) {
                    assertEquals("name from same type " + type1.getDescription() + " should be valid", true, type2.isValidName(currBackupName));
                } else {
                    assertEquals("name from different type should be invalid", false, type2.isValidName(currBackupName));
                }
            }
        }
    }

    @Test
    public void sortBackupsByTimeTest() throws Exception {
        // Make sure that the sortBackupsByTime method correctly sorts backups from most recent to earliest
        long now = System.currentTimeMillis();
        String newGold = new GoldBackup().generateCurrentName(true);
        String olderHourly = new HourlyBackup().generateNameForTime(now - 100_000, true);
        String evenOlderGold = new GoldBackup().generateNameForTime(now - 200_000, true);
        String oldestDaily = new DailyBackup().generateNameForTime(now - 400_000, true);
        ArrayList<String> backupsRandomOrder = new ArrayList<>(Arrays.asList(olderHourly, newGold, evenOlderGold, oldestDaily));
        ScheduledBackupType.sortBackupsByTime(backupsRandomOrder);
        List<String> expected = Arrays.asList(newGold, olderHourly, evenOlderGold, oldestDaily);
        for (int i=0; i<expected.size(); i++)
        {
            assertEquals("should get list in expected order", expected.get(i), backupsRandomOrder.get(i));
        }
    }
}