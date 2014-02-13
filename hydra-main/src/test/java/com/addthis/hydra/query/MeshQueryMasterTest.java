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
package com.addthis.hydra.query;


import java.io.File;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.addthis.basis.util.Files;

import com.addthis.hydra.query.util.MeshSourceAggregator;
import com.addthis.hydra.query.util.QueryData;
import com.addthis.meshy.service.file.FileReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MeshQueryMasterTest {

    private MeshQueryMaster meshQueryMaster;
    private MeshFileRefCache cachey;
    private String tmpRoot;

    @Before
    public void before() throws Exception {
        tmpRoot = Files.createTempDir().toString();
        String tmpDir = tmpRoot + "/mqmastertest";
        Files.initDirectory(tmpDir);

        System.setProperty("qmaster.log.dir", tmpDir);
        System.setProperty("qmaster.web.dir", tmpDir);
        System.setProperty("qmaster.data.dir", tmpDir);
        System.setProperty("qmaster.temp.dir", tmpDir);
        System.setProperty("QueryCache.LOG_DIR", tmpDir);
        System.setProperty("qmaster.enableZooKeeper", "false");
        System.setProperty("qmaster.log.accessLogDir", tmpDir);
        meshQueryMaster = new MeshQueryMaster(null);
        cachey = new MeshFileRefCache();
    }

    @After
    public void after() throws InterruptedException {
        meshQueryMaster.shutdown();
        Files.deleteDir(new File(tmpRoot));
    }

    @Test
    public void testFilterFileReferenceMap_happyPath() throws Exception {

        Map<Integer, Set<FileReferenceWrapper>> fileReferenceMap = new HashMap<Integer, Set<FileReferenceWrapper>>();
        for (int i = 0; i < 5; i++) {
            Set<FileReferenceWrapper> fileReferenceWrappers = new HashSet<FileReferenceWrapper>();
            for (int j = 0; j < 3; j++) {
                fileReferenceWrappers.add(new FileReferenceWrapper(new FileReference("test" + i + ":" + j, 5000, 5000), i));
            }
            fileReferenceMap.put(i, fileReferenceWrappers);
        }
        Map<Integer, Set<FileReferenceWrapper>> filteredFileReferenceMap = cachey.filterFileReferences(fileReferenceMap);
        assertEquals(fileReferenceMap.size(), filteredFileReferenceMap.size());
        for (Map.Entry<Integer, Set<FileReferenceWrapper>> entry : fileReferenceMap.entrySet()) {
            Set<FileReferenceWrapper> fileReferenceWrappers = entry.getValue();
            Set<FileReferenceWrapper> filteredFileReferenceWrappers = filteredFileReferenceMap.get(entry.getKey());
            assertEquals(fileReferenceWrappers.size(), filteredFileReferenceWrappers.size());
        }
    }

    @Test
    public void testFilterFileReferenceMap_oneOldFile() throws Exception {
        Map<Integer, Set<FileReferenceWrapper>> fileReferenceMap = new HashMap<Integer, Set<FileReferenceWrapper>>();
        for (int i = 0; i < 5; i++) {
            Set<FileReferenceWrapper> fileReferenceWrappers = new HashSet<FileReferenceWrapper>();
            for (int j = 0; j < 3; j++) {
                if (i == 1 && j == 1) {
                    fileReferenceWrappers.add(new FileReferenceWrapper(new FileReference("test" + i + ":" + j, 1000, 5000), i));
                } else {
                    fileReferenceWrappers.add(new FileReferenceWrapper(new FileReference("test" + i + ":" + j, 5000, 5000), i));
                }
            }
            fileReferenceMap.put(i, fileReferenceWrappers);
        }
        Map<Integer, Set<FileReferenceWrapper>> filteredFileReferenceMap = cachey.filterFileReferences(fileReferenceMap);
        assertEquals(fileReferenceMap.size(), filteredFileReferenceMap.size());
        for (Map.Entry<Integer, Set<FileReferenceWrapper>> entry : fileReferenceMap.entrySet()) {
            Set<FileReferenceWrapper> fileReferenceWrappers = entry.getValue();
            Set<FileReferenceWrapper> filteredFileReferenceWrappers = filteredFileReferenceMap.get(entry.getKey());
            if (entry.getKey() == 1) {
                assertEquals(fileReferenceWrappers.size() - 1, filteredFileReferenceWrappers.size());
                assertEquals(fileReferenceWrappers.iterator().next().fileReference.lastModified, 5000);
            } else {
                assertEquals(fileReferenceWrappers.size(), filteredFileReferenceWrappers.size());
            }
        }
    }

//	@Test
//	public void testFilterFileReferenceMap_oneSmallFile() throws Exception
//	{
//		Map<Integer, Set<FileReferenceWrapper>> fileReferenceMap = new HashMap<Integer, Set<FileReferenceWrapper>>();
//		for (int i = 0; i < 5; i++)
//		{
//			Set<FileReferenceWrapper> fileReferenceWrappers = new HashSet<FileReferenceWrapper>();
//			for (int j = 0; j < 3; j++)
//			{
//				if (i == 1 && j == 1)
//				{
//					fileReferenceWrappers.add(new FileReferenceWrapper(new FileService.FileReference("test" + i + ":" + j, 5000, 1000), i));
//				}
//				else
//				{
//					fileReferenceWrappers.add(new FileReferenceWrapper(new FileService.FileReference("test" + i + ":" + j, 5000, 5000), i));
//				}
//			}
//			fileReferenceMap.put(i, fileReferenceWrappers);
//		}
//		Map<Integer, Set<FileReferenceWrapper>> filteredFileReferenceMap = meshQueryMaster.filterFileReferences(fileReferenceMap);
//		assertEquals(fileReferenceMap.size(), filteredFileReferenceMap.size());
//		for (Map.Entry<Integer, Set<FileReferenceWrapper>> entry : fileReferenceMap.entrySet())
//		{
//			Set<FileReferenceWrapper> fileReferenceWrappers = entry.getValue();
//			Set<FileReferenceWrapper> filteredFileReferenceWrappers = filteredFileReferenceMap.get(entry.getKey());
//			if (entry.getKey() == 1)
//			{
//				assertEquals(fileReferenceWrappers.size() - 1, filteredFileReferenceWrappers.size());
//				assertEquals(fileReferenceWrappers.iterator().next().fileReference.size, 5000);
//			}
//			else
//			{
//				assertEquals(fileReferenceWrappers.size(), filteredFileReferenceWrappers.size());
//			}
//		}
//	}

    @Test
    public void testFilterFileReferenceMap_oneOldAndSmallFile() throws Exception {
        Map<Integer, Set<FileReferenceWrapper>> fileReferenceMap = new HashMap<Integer, Set<FileReferenceWrapper>>();
        for (int i = 0; i < 5; i++) {
            Set<FileReferenceWrapper> fileReferenceWrappers = new HashSet<FileReferenceWrapper>();
            for (int j = 0; j < 3; j++) {
                if (i == 1 && j == 1) {
                    fileReferenceWrappers.add(new FileReferenceWrapper(new FileReference("test" + i + ":" + j, 1000, 1000), i));
                } else {
                    fileReferenceWrappers.add(new FileReferenceWrapper(new FileReference("test" + i + ":" + j, 5000, 5000), i));
                }
            }
            fileReferenceMap.put(i, fileReferenceWrappers);
        }
        Map<Integer, Set<FileReferenceWrapper>> filteredFileReferenceMap = cachey.filterFileReferences(fileReferenceMap);
        assertEquals(fileReferenceMap.size(), filteredFileReferenceMap.size());
        for (Map.Entry<Integer, Set<FileReferenceWrapper>> entry : fileReferenceMap.entrySet()) {
            Set<FileReferenceWrapper> fileReferenceWrappers = entry.getValue();
            Set<FileReferenceWrapper> filteredFileReferenceWrappers = filteredFileReferenceMap.get(entry.getKey());
            if (entry.getKey() == 1) {
                assertEquals(fileReferenceWrappers.size() - 1, filteredFileReferenceWrappers.size());
                assertEquals(fileReferenceWrappers.iterator().next().fileReference.size, 5000);
            } else {
                assertEquals(fileReferenceWrappers.size(), filteredFileReferenceWrappers.size());
            }
        }
    }

    @Test
    public void testAllocateQueryTask_happyPath() throws Exception {
        Map<String, Integer> queryTaskCountMap = new HashMap<String, Integer>();
        queryTaskCountMap.put("h1", 1);
        queryTaskCountMap.put("h2", 0);
        FileReference fileReference1 = new FileReference("test1", 1000, 1000) {{ setHostUUID("h1"); }};
        FileReference fileReference2 = new FileReference("test1", 1000, 1000) {{ setHostUUID("h2"); }};
        HashMap<String, Boolean> readOnlyHostMap = new HashMap<String, Boolean>();
        HashSet<QueryData> queryDataSet = new HashSet<QueryData>();
        queryDataSet.add(new QueryData(null, fileReference1, null, "jobid", 0));
        queryDataSet.add(new QueryData(null, fileReference2, null, "jobid", 0));
        QueryData bestQueryData = MeshSourceAggregator.allocateQueryTaskLegacy(queryTaskCountMap, queryDataSet, readOnlyHostMap);
        assertEquals(bestQueryData.hostEntryInfo.getHostName(), fileReference2.getHostUUID());
    }

//	@Test
//	public void testAllocateQueryTask_readOnly() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
//	{
//		Map<String, Integer> queryTaskCountMap = new HashMap<String, Integer>();
//		queryTaskCountMap.put("h1", 1);
//		queryTaskCountMap.put("h2", 0);
//		FileService.FileReference fileReference1 = new FileService.FileReference("test1", 1000, 1000);
//		Method setHostUUID = fileReference1.getClass().getDeclaredMethod("setHostUUID", String.class);
//		setHostUUID.setAccessible(true);
//		setHostUUID.invoke(fileReference1, "h1");
//		FileService.FileReference fileReference2 = new FileService.FileReference("test1", 1000, 1000);
//		setHostUUID = fileReference2.getClass().getDeclaredMethod("setHostUUID", String.class);
//		setHostUUID.setAccessible(true);
//		setHostUUID.invoke(fileReference2, "h2");
//		HashMap<String, Boolean> readOnlyHostMap = new HashMap<String, Boolean>();
//		readOnlyHostMap.put("h1", true);
//		HashSet<QueryData> queryDataSet = new HashSet<QueryData>();
//		queryDataSet.add(new QueryData(null, fileReference1, null, 0));
//		queryDataSet.add(new QueryData(null, fileReference2, null, 0));
//		QueryData bestQueryData = MeshSourceAggregator.allocateQueryTaskUsingHostMetrics(queryTaskCountMap, queryDataSet, readOnlyHostMap);
//		assertEquals(bestQueryData.hostEntryInfo.getHostName(), fileReference1.getHostUUID());
//	}
//
//	@Test
//	public void testAllocateQueryTask_readOnly2() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
//	{
//		// in this test there are two read only hosts available
//		// the host with a smaller number of tasks already assigned should be selected
//		Map<String, Integer> queryTaskCountMap = new HashMap<String, Integer>();
//		queryTaskCountMap.put("h1", 2);
//		queryTaskCountMap.put("h2", 0);
//		queryTaskCountMap.put("h3", 1);
//		FileService.FileReference fileReference1 = new FileService.FileReference("test1", 1000, 1000);
//		Method setHostUUID = fileReference1.getClass().getDeclaredMethod("setHostUUID", String.class);
//		setHostUUID.setAccessible(true);
//		setHostUUID.invoke(fileReference1, "h1");
//		FileService.FileReference fileReference2 = new FileService.FileReference("test1", 1000, 1000);
//		setHostUUID = fileReference2.getClass().getDeclaredMethod("setHostUUID", String.class);
//		setHostUUID.setAccessible(true);
//		setHostUUID.invoke(fileReference2, "h2");
//		FileService.FileReference fileReference3 = new FileService.FileReference("test1", 1000, 1000);
//		setHostUUID = fileReference3.getClass().getDeclaredMethod("setHostUUID", String.class);
//		setHostUUID.setAccessible(true);
//		setHostUUID.invoke(fileReference3, "h3");
//		HashMap<String, Boolean> readOnlyHostMap = new HashMap<String, Boolean>();
//		readOnlyHostMap.put("h1", true);
//		readOnlyHostMap.put("h3", true);
//		HashSet<QueryData> queryDataSet = new HashSet<QueryData>();
//		queryDataSet.add(new QueryData(null, fileReference1, null, 0));
//		queryDataSet.add(new QueryData(null, fileReference2, null, 0));
//		queryDataSet.add(new QueryData(null, fileReference3, null, 0));
//		QueryData bestQueryData = MeshSourceAggregator.allocateQueryTaskUsingHostMetrics(queryTaskCountMap, queryDataSet, readOnlyHostMap);
//		assertEquals(bestQueryData.hostEntryInfo.getHostName(), fileReference3.getHostUUID());
//	}

}
