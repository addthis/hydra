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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.addthis.basis.util.Files;

import com.addthis.meshy.service.file.FileReference;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MeshQueryMasterTest {

    private MeshQueryMaster meshQueryMaster;
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
    }

    @After
    public void after() throws InterruptedException {
        meshQueryMaster.shutdown();
        Files.deleteDir(new File(tmpRoot));
    }

    @Test
    public void testFilterFileReferenceMap_happyPath() throws Exception {

        Multimap<Integer, FileReference> fileReferenceMap = HashMultimap.create();
        for (int i = 0; i < 5; i++) {
            Set<FileReference> fileReferenceWrappers = new HashSet<>();
            for (int j = 0; j < 3; j++) {
                fileReferenceWrappers.add(new FileReference("test" + i + ":" + j, 5000, 5000));
            }
            fileReferenceMap.putAll(i, fileReferenceWrappers);
        }
        Multimap<Integer, FileReference> filteredFileReferenceMap = MeshFileRefCache.filterFileReferences(
                fileReferenceMap);
        assertEquals(fileReferenceMap.size(), filteredFileReferenceMap.size());
        for (Map.Entry<Integer, Collection<FileReference>> entry : fileReferenceMap.asMap().entrySet()) {
            Collection<FileReference> fileReferenceWrappers = entry.getValue();
            Collection<FileReference> filteredFileReferences = filteredFileReferenceMap.get(entry.getKey());
            assertEquals(fileReferenceWrappers.size(), filteredFileReferences.size());
        }
    }

    @Test
    public void testFilterFileReferenceMap_oneOldFile() throws Exception {
        Multimap<Integer, FileReference> fileReferenceMap = HashMultimap.create();
        for (int i = 0; i < 5; i++) {
            Set<FileReference> fileReferenceWrappers = new HashSet<>();
            for (int j = 0; j < 3; j++) {
                if (i == 1 && j == 1) {
                    fileReferenceWrappers.add(new FileReference("test" + i + ":" + j, 1000, 5000));
                } else {
                    fileReferenceWrappers.add(new FileReference("test" + i + ":" + j, 5000, 5000));
                }
            }
            fileReferenceMap.putAll(i, fileReferenceWrappers);
        }
        Multimap<Integer, FileReference> filteredFileReferenceMap =
                MeshFileRefCache.filterFileReferences(fileReferenceMap);

        assertEquals(fileReferenceMap.keySet(), filteredFileReferenceMap.keySet());
        for (Map.Entry<Integer, Collection<FileReference>> entry : fileReferenceMap.asMap().entrySet()) {
            Collection<FileReference> fileReferences = entry.getValue();
            Collection<FileReference> filteredFileReferences = filteredFileReferenceMap.get(entry.getKey());
            if (entry.getKey() == 1) {
                assertEquals(fileReferences.size() - 1, filteredFileReferences.size());
                assertEquals(5000, filteredFileReferences.iterator().next().lastModified);
            } else {
                assertEquals(fileReferences.size(), filteredFileReferences.size());
            }
        }
    }

//	@Test
//	public void testFilterFileReferenceMap_oneSmallFile() throws Exception
//	{
//		Multimap<Integer, FileReference> fileReferenceMap = new HashMultimap<Integer, FileReference>();
//		for (int i = 0; i < 5; i++)
//		{
//			Set<FileReference> fileReferenceWrappers = new HashSet<FileReference>();
//			for (int j = 0; j < 3; j++)
//			{
//				if (i == 1 && j == 1)
//				{
//					fileReferenceWrappers.add(new FileReference(new FileService.FileReference("test" + i + ":" + j, 5000, 1000), i));
//				}
//				else
//				{
//					fileReferenceWrappers.add(new FileReference(new FileService.FileReference("test" + i + ":" + j, 5000, 5000), i));
//				}
//			}
//			fileReferenceMap.put(i, fileReferenceWrappers);
//		}
//		Multimap<Integer, FileReference> filteredFileReferenceMap = meshQueryMaster.filterFileReferences(fileReferenceMap);
//		assertEquals(fileReferenceMap.size(), filteredFileReferenceMap.size());
//		for (Map.Entry<Integer, Set<FileReference>> entry : fileReferenceMap.entrySet())
//		{
//			Set<FileReference> fileReferenceWrappers = entry.getValue();
//			Set<FileReference> filteredFileReferences = filteredFileReferenceMap.get(entry.getKey());
//			if (entry.getKey() == 1)
//			{
//				assertEquals(fileReferenceWrappers.size() - 1, filteredFileReferences.size());
//				assertEquals(fileReferenceWrappers.iterator().next().fileReference.size, 5000);
//			}
//			else
//			{
//				assertEquals(fileReferenceWrappers.size(), filteredFileReferences.size());
//			}
//		}
//	}

    @Test
    public void testFilterFileReferenceMap_oneOldAndSmallFile() throws Exception {
        Multimap<Integer, FileReference> fileReferenceMap = HashMultimap.create();
        for (int i = 0; i < 5; i++) {
            Set<FileReference> fileReferenceWrappers = new HashSet<>();
            for (int j = 0; j < 3; j++) {
                if (i == 1 && j == 1) {
                    fileReferenceWrappers.add(new FileReference("test" + i + ":" + j, 1000, 1000));
                } else {
                    fileReferenceWrappers.add(new FileReference("test" + i + ":" + j, 5000, 5000));
                }
            }
            fileReferenceMap.putAll(i, fileReferenceWrappers);
        }
        Multimap<Integer, FileReference> filteredFileReferenceMap =
                MeshFileRefCache.filterFileReferences(fileReferenceMap);

        assertEquals(fileReferenceMap.keySet(), filteredFileReferenceMap.keySet());
        for (Map.Entry<Integer, Collection<FileReference>> entry : fileReferenceMap.asMap().entrySet()) {
            Collection<FileReference> fileReferences = entry.getValue();
            Collection<FileReference> filteredFileReferences = filteredFileReferenceMap.get(entry.getKey());
            if (entry.getKey() == 1) {
                assertEquals(fileReferences.size() - 1, filteredFileReferences.size());
                assertEquals(5000, filteredFileReferences.iterator().next().size);
            } else {
                assertEquals(fileReferences, filteredFileReferences);
            }
        }
    }

    @Test
    public void testAllocateQueryTask_happyPath() throws Exception {
//        Map<String, Integer> queryTaskCountMap = new HashMap<String, Integer>();
//        queryTaskCountMap.put("h1", 1);
//        queryTaskCountMap.put("h2", 0);
//        FileReference fileReference1 = new FileReference("test1", 1000, 1000) {{ setHostUUID("h1"); }};
//        FileReference fileReference2 = new FileReference("test1", 1000, 1000) {{ setHostUUID("h2"); }};
//        HashMap<String, Boolean> readOnlyHostMap = new HashMap<String, Boolean>();
//        HashSet<QueryData> queryDataSet = new HashSet<QueryData>();
//        queryDataSet.add(new QueryData(null, fileReference1, null, "jobid", 0));
//        queryDataSet.add(new QueryData(null, fileReference2, null, "jobid", 0));
//        QueryData bestQueryData = TaskAllocator.allocateQueryTaskLegacy(queryTaskCountMap, queryDataSet, readOnlyHostMap);
//        assertEquals(bestQueryData.hostEntryInfo.getHostName(), fileReference2.getHostUUID());
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
