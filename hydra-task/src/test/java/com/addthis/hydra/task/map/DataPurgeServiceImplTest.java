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

import java.io.File;
import java.io.IOException;

import java.util.List;

import com.addthis.basis.util.Files;

import org.apache.commons.io.FileUtils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class DataPurgeServiceImplTest {

    private static final Logger logger = LoggerFactory.getLogger(DataPurgeServiceImplTest.class);


    private File tmpPrefix;
    DataPurgeServiceImpl dataPurgeService;

    public String getPrefix() {
        return tmpPrefix + "/purgetest";
        //return "/tmp/purgetest";
    }

    @Before
    public void before() throws IOException {
        tmpPrefix = Files.createTempDir(); //  = /tmp
        dataPurgeService = new DataPurgeServiceImpl();
    }

    @After
    public void after() {
        FileUtils.deleteQuietly(tmpPrefix);
    }

    @Test
    public void testArgumentValidation() throws Exception {
        DataPurgeConfig dataPurgeConfig = new DataPurgeConfig();
        dataPurgeConfig.setMaxAgeInDays(1);

        assertFalse(dataPurgeService.purgeData(dataPurgeConfig, new DateTime()));
        dataPurgeConfig.setDirectoryPrefix(new String[]{"base"});
        assertFalse(dataPurgeService.purgeData(dataPurgeConfig, new DateTime()));
        dataPurgeConfig.setDatePathFormat("yy/MM/dd");
        assertFalse(dataPurgeService.purgeData(dataPurgeConfig, null));
        dataPurgeConfig.setMaxAgeInDays(-1);
        assertFalse(dataPurgeService.purgeData(dataPurgeConfig, new DateTime()));
        dataPurgeConfig.setMaxAgeInDays(1);
        dataPurgeConfig.setDatePathFormat(null);
        assertFalse(dataPurgeService.purgeData(dataPurgeConfig, new DateTime()));
        dataPurgeConfig.setDatePathFormat("yy/MM/dd");
        dataPurgeConfig.setFileBasedPurge(true);
        assertFalse(dataPurgeService.purgeData(dataPurgeConfig, new DateTime()));
        dataPurgeConfig.setDateStartIndex(2);
        assertFalse(dataPurgeService.purgeData(dataPurgeConfig, new DateTime()));
        dataPurgeConfig.setDateStringLength(4);
        assertTrue(dataPurgeService.purgeData(dataPurgeConfig, new DateTime()));
    }

    @Test
    public void testExpandPrefix() throws Exception {
        String prefix = getPrefix() + "/dptest/";
        createNovDecData(prefix);
        String test1 = prefix + "*";
        String test2 = prefix + "*/*";
        String test3 = prefix + "*/*/*";
        String test4 = prefix + "11/01/*";
        String test5 = prefix + "*/01";
        String test6 = prefix + "*/01/*";
        String test7 = prefix + "*1/*/01";
        List<File> result1 = dataPurgeService.expandPrefix(test1);
        List<File> result2 = dataPurgeService.expandPrefix(test2);
        List<File> result3 = dataPurgeService.expandPrefix(test3);
        List<File> result4 = dataPurgeService.expandPrefix(test4);
        List<File> result5 = dataPurgeService.expandPrefix(test5);
        List<File> result6 = dataPurgeService.expandPrefix(test6);
        List<File> result7 = dataPurgeService.expandPrefix(test7);
        assertEquals(result1.size(), 2);
        assertEquals(result2.size(), 4);
        assertEquals(result3.size(), 12);
        assertEquals(result4.size(), 3);
        assertEquals(result5.size(), 2);
        assertEquals(result6.size(), 6);
        assertEquals(result7.size(), 2);
    }

    @Test
    public void testGetDirectoryList() throws IOException {
        String prefix = getPrefix() + "/dptest";
        createData(prefix);
        List<File> directoryList = dataPurgeService.getSubdirectoryList(new File(prefix), null);
        assertNotNull(directoryList);
        assertEquals(9, directoryList.size());
    }

    @Test
    public void testPurgeData() throws IOException {
        String prefix = getPrefix() + "/dptest";
        createData(prefix);
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yy/MM/dd");
        DateTime currentTime = formatter.parseDateTime("11/01/04");
        DataPurgeConfig dataPurgeConfig = new DataPurgeConfig();
        dataPurgeConfig.setMaxAgeInDays(1);
        dataPurgeConfig.setDirectoryPrefix(new String[]{prefix});
        dataPurgeConfig.setDatePathFormat("yy/MM/dd");
        assertTrue(dataPurgeService.purgeData(dataPurgeConfig, currentTime));
        List<File> directoryList = dataPurgeService.getSubdirectoryList(new File(prefix), null);
        assertNotNull(directoryList);
        assertEquals(7, directoryList.size());
    }

    @Test
    public void testWildcardPurgeData() throws IOException {
        String tmp = getPrefix();
        String prefix = tmp + "/split/search-*-hashed/*";
        createData(tmp + "/split/search-us-hashed/0");
        createData(tmp + "/split/search-us-hashed/1");
        createData(tmp + "/split/search-gb-hashed/0");
        createData(tmp + "/split/search-gb-hashed/1");
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yy/MM/dd");
        DateTime currentTime = formatter.parseDateTime("11/01/04");
        DataPurgeConfig dataPurgeConfig = new DataPurgeConfig();
        dataPurgeConfig.setMaxAgeInDays(1);
        dataPurgeConfig.setDirectoryPrefix(new String[]{prefix});
        dataPurgeConfig.setDatePathFormat("yy/MM/dd");
        assertTrue(dataPurgeService.purgeData(dataPurgeConfig, currentTime));
        List<File> directoryList = dataPurgeService.generateDirectoryList(prefix);
        assertNotNull(directoryList);
        assertEquals(28, directoryList.size());
    }

    @Test
    public void testPurgeDataHourly() throws IOException {
        String prefix = getPrefix() + "/ht";
        createHourlyData(prefix);
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyMMdd/HH");
        DateTime currentTime = formatter.parseDateTime("120326/13");
        DataPurgeConfig dataPurgeConfig = new DataPurgeConfig();
        dataPurgeConfig.setCleanEmptyParents(true);
        dataPurgeConfig.setMaxAgeInHours(22);
        dataPurgeConfig.setDirectoryPrefix(new String[]{prefix});
        dataPurgeConfig.setDatePathFormat("yyMMdd/HH");
        assertTrue(dataPurgeService.purgeData(dataPurgeConfig, currentTime));
        List<File> directoryList = dataPurgeService.getSubdirectoryList(new File(prefix), null);
        logger.trace("remaining after hourly purge {}", directoryList);
        assertNotNull(directoryList);
        assertEquals(6, directoryList.size()); // includes sub-directories
    }


    @Test
    public void testPurgeData_Files() throws IOException {
        String prefix = getPrefix() + "/dptest";
        createFileBasedData(prefix);
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyMMdd");
        DateTime currentTime = formatter.parseDateTime("110104");
        DataPurgeConfig dataPurgeConfig = new DataPurgeConfig();
        dataPurgeConfig.setMaxAgeInDays(1);
        dataPurgeConfig.setDirectoryPrefix(new String[]{prefix});
        dataPurgeConfig.setDatePathFormat("yyMMdd");
        dataPurgeConfig.setFileBasedPurge(true);
        dataPurgeConfig.setDateStartIndex(9);
        dataPurgeConfig.setDateStringLength(6);
        assertTrue(dataPurgeService.purgeData(dataPurgeConfig, currentTime));
        List<File> directoryList = dataPurgeService.getSubdirectoryList(new File(prefix), null);
        assertNotNull(directoryList);
        assertEquals(1, directoryList.size());
        File[] fileList = directoryList.iterator().next().listFiles();
        assertNotNull(fileList);
        assertEquals(fileList.length, 4);
    }

    @Test
    public void testPurgeData_Files_TwoPrefixDirs() throws IOException {
        String prefix = getPrefix() + "/dptest";
        String prefix2 = getPrefix() + "/dptest2";
        createFileBasedData(prefix);
        createFileBasedData(prefix2);
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyMMdd");
        DateTime currentTime = formatter.parseDateTime("110104");
        DataPurgeConfig dataPurgeConfig = new DataPurgeConfig();
        dataPurgeConfig.setMaxAgeInDays(1);
        dataPurgeConfig.setDirectoryPrefix(new String[]{prefix, prefix2});
        dataPurgeConfig.setDatePathFormat("yyMMdd");
        dataPurgeConfig.setFileBasedPurge(true);
        dataPurgeConfig.setDateStartIndex(9);
        dataPurgeConfig.setDateStringLength(6);

        assertTrue(dataPurgeService.purgeData(dataPurgeConfig, currentTime));
        List<File> directoryList = dataPurgeService.getSubdirectoryList(new File(prefix), null);
        assertNotNull(directoryList);
        assertEquals(1, directoryList.size());
        File[] fileList = directoryList.iterator().next().listFiles();
        assertNotNull(fileList);
        assertEquals(fileList.length, 4);

        directoryList = dataPurgeService.getSubdirectoryList(new File(prefix2), null);
        assertNotNull(directoryList);
        assertEquals(1, directoryList.size());
        fileList = directoryList.iterator().next().listFiles();
        assertNotNull(fileList);
        assertEquals(fileList.length, 4);
    }

    @Test
    public void testShouldDelete() throws IOException {
        String prefix = getPrefix() + "/dptest";
        createData(prefix);
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yy/MM/dd");
        DateTime oldest = formatter.parseDateTime("11/01/03");
        assertTrue(dataPurgeService.shouldDelete(formatter, oldest, "11/01/01"));
        assertTrue(dataPurgeService.shouldDelete(formatter, oldest, "11/01/02"));
        assertFalse(dataPurgeService.shouldDelete(formatter, oldest, "11/01/03"));
        assertFalse(dataPurgeService.shouldDelete(formatter, oldest, "11/01/04"));
        assertTrue(dataPurgeService.shouldDelete(formatter, oldest, "10/01/04"));
        assertFalse(dataPurgeService.shouldDelete(formatter, oldest, "11/04/04"));
        assertFalse(dataPurgeService.shouldDelete(formatter, oldest, "12/04/04"));
    }

    private void createData(String prefix) throws IOException {
        new File(prefix + "/11/01/01").mkdirs();
        new File(prefix + "/11/01/01/testFile.txt").createNewFile();
        new File(prefix + "/11/01/02").mkdirs();
        new File(prefix + "/11/01/02/testFile.txt").createNewFile();
        new File(prefix + "/11/01/03").mkdirs();
        new File(prefix + "/11/01/03/testFile.txt").createNewFile();
        new File(prefix + "/11/01/04").mkdirs();
        new File(prefix + "/11/01/04/testFile.txt").createNewFile();
        new File(prefix + "/11/01/05").mkdirs();
        new File(prefix + "/11/01/05/testFile.txt").createNewFile();
        new File(prefix + "/11/01/06").mkdirs();
        new File(prefix + "/11/01/06/testFile.txt").createNewFile();
    }

    private void createNovDecData(String prefix) throws IOException {
        new File(prefix + "/11/01/01").mkdirs();
        new File(prefix + "/11/01/01/testFile.txt").createNewFile();
        new File(prefix + "/11/01/02").mkdirs();
        new File(prefix + "/11/01/02/testFile.txt").createNewFile();
        new File(prefix + "/11/01/03").mkdirs();
        new File(prefix + "/11/01/03/testFile.txt").createNewFile();
        new File(prefix + "/11/02/01").mkdirs();
        new File(prefix + "/11/02/01/testFile.txt").createNewFile();
        new File(prefix + "/11/02/02").mkdirs();
        new File(prefix + "/11/02/02/testFile.txt").createNewFile();
        new File(prefix + "/11/02/03").mkdirs();
        new File(prefix + "/11/02/03/testFile.txt").createNewFile();
        new File(prefix + "/12/01/01").mkdirs();
        new File(prefix + "/12/01/01/testFile.txt").createNewFile();
        new File(prefix + "/12/01/02").mkdirs();
        new File(prefix + "/12/01/02/testFile.txt").createNewFile();
        new File(prefix + "/12/01/03").mkdirs();
        new File(prefix + "/12/01/03/testFile.txt").createNewFile();
        new File(prefix + "/12/02/01").mkdirs();
        new File(prefix + "/12/02/01/testFile.txt").createNewFile();
        new File(prefix + "/12/02/02").mkdirs();
        new File(prefix + "/12/02/02/testFile.txt").createNewFile();
        new File(prefix + "/12/02/03").mkdirs();
        new File(prefix + "/12/02/03/testFile.txt").createNewFile();
    }


    private void createHourlyData(String prefix) throws IOException {
        new File(prefix + "/120324/12").mkdirs();
        new File(prefix + "/120324/12/testFile.txt").createNewFile();
        new File(prefix + "/120325/12").mkdirs();
        new File(prefix + "/120325/12/testFile.txt").createNewFile();
        new File(prefix + "/120325/13").mkdirs();
        new File(prefix + "/120325/13/testFile.txt").createNewFile();
        new File(prefix + "/120325/14").mkdirs();
        new File(prefix + "/120325/14/testFile.txt").createNewFile();
        new File(prefix + "/120325/15").mkdirs();
        new File(prefix + "/120325/15/testFile.txt").createNewFile();
        new File(prefix + "/120325/16").mkdirs();
        new File(prefix + "/120325/16/testFile.txt").createNewFile();
        new File(prefix + "/120326/08").mkdirs();
        new File(prefix + "/120326/08/testFile.txt").createNewFile();

    }


    private void createFileBasedData(String prefix) throws IOException {
        new File(prefix).mkdirs();
        new File(prefix + "/testFile-110101.txt").createNewFile();
        new File(prefix + "/testFile-110102.txt").createNewFile();
        new File(prefix + "/testFile-110103.txt").createNewFile();
        new File(prefix + "/testFile-110104.txt").createNewFile();
        new File(prefix + "/testFile-110105.txt").createNewFile();
        new File(prefix + "/testFile-110106.txt").createNewFile();
    }
}
