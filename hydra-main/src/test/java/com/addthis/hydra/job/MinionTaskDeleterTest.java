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

import java.io.File;

import java.util.Arrays;
import java.util.List;

import com.addthis.basis.util.Files;

import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.backup.BackupToDelete;
import com.addthis.hydra.job.backup.DailyBackup;
import com.addthis.hydra.job.backup.GoldBackup;
import com.addthis.hydra.job.backup.ScheduledBackupType;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MinionTaskDeleterTest {

    private static final long now = System.currentTimeMillis();
    private static final long dayInMillis = 86400000;

    /**
     * We should only delete gold backups when they're old enough. Other backups should be deleted regardless.
     */
    @Test
    public void shouldDeleteBackupTest() {
        GoldBackup goldBackup = new GoldBackup();
        Long goldProtectionTime = ScheduledBackupType.getProtectedBackupTypes().get(goldBackup);
        assertTrue("gold backups should be protected", goldProtectionTime != null);
        String newGoldBackup = goldBackup.generateNameForTime(now, true);
        String youngGoldBackup = goldBackup.generateNameForTime(now - goldProtectionTime / 2, true);
        String oldGoldBackup = goldBackup.generateNameForTime(now - 3 * goldProtectionTime, true);
        assertTrue("should not delete new gold backup", !MinionTaskDeleter.shouldDeleteBackup(newGoldBackup, goldBackup));
        assertTrue("should not delete young gold backup", !MinionTaskDeleter.shouldDeleteBackup(youngGoldBackup, goldBackup));
        assertTrue("should delete old gold backup", MinionTaskDeleter.shouldDeleteBackup(oldGoldBackup, goldBackup));
        DailyBackup dailyBackup = new DailyBackup();
        assertTrue("should delete old daily backup", MinionTaskDeleter.shouldDeleteBackup(dailyBackup.generateNameForTime(now - dayInMillis, true), dailyBackup));
    }

    /**
     * If we want to delete a task, we should delete everything besides young gold directories immediately.
     *
     * @throws Exception on failure
     */
    @Test
    public void taskDeleteTest() throws Exception {
        File tmpDir = Files.createTempDir();
        try {
            GoldBackup goldBackup = new GoldBackup();
            List<String> directories = Arrays.asList("live", "config", "gold", goldBackup.generateNameForTime(now - dayInMillis, true), goldBackup.generateNameForTime(now, true));
            String basePath = tmpDir.getAbsolutePath() + "/";
            for (String dir : directories) {
                assertTrue(new File(basePath + dir).mkdir());
            }
            MinionTaskDeleter del = new MinionTaskDeleter();
            del.submitPathToDelete(tmpDir.getAbsolutePath());
            del.deleteStoredItems();
            assertArrayEquals("should have only recent b-gold directory remaining", tmpDir.list(), new String[]{directories.get(4)});
            assertEquals("should have remaining backup in backupsToDelete", del.getBackupsToDelete(), ImmutableSet.of(new BackupToDelete(basePath + directories.get(4), "gold")));
        } finally {
            assertTrue(Files.deleteDir(tmpDir));
        }
    }

    /**
     * MinionTaskDeleter should persist its stored lists of tasks/backups
     *
     * @throws Exception on failure
     */
    @Test
    public void persistTest() throws Exception {
        MinionTaskDeleter del = new MinionTaskDeleter();
        String taskPath = "some/task/path";
        GoldBackup goldBackup = new GoldBackup();
        String backupPath = "some/othertask/path/" + goldBackup.generateCurrentName(true);
        del.submitPathToDelete(taskPath);
        del.submitBackupToDelete(backupPath, goldBackup);
        CodecJSON codec = CodecJSON.INSTANCE;
        byte[] serBytes = codec.encode(del);
        MinionTaskDeleter del2 = new MinionTaskDeleter();
        codec.decode(del2, serBytes);
        assertEquals("should persist backups to delete", ImmutableSet.of(new BackupToDelete(backupPath, "gold")), del2.getBackupsToDelete());
        assertEquals("should persist tasks to delete", ImmutableSet.of(taskPath), del.getTasksToDelete());

    }
}
