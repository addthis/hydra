package com.addthis.hydra.query.spawndatastore;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.job.store.AvailableCache;
import com.addthis.hydra.job.store.DataStoreUtil;
import com.addthis.hydra.job.store.SpawnDataStore;

import com.google.common.base.Strings;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AliasCache {
    private static final Logger log = LoggerFactory.getLogger(AliasCache.class);

    /* The interval to refresh cached alias values */
    private static long DEFAULT_REFRESH_INTERVAL = Parameter.longValue("alias.bimap.refresh", 200);
    /* The expiration period for cache values. Off by default, but useful for testing. */
    private static long DEFAULT_CACHE_EXPIRE = Parameter.longValue("alias.bimap.expire", -1);
    /* The max size of the alias cache */
    private static int DEFAULT_CACHE_SIZE = Parameter.intValue("alias.bimap.cache.size", 1000);

    private static final String ALIAS_PATH = "/query/alias";

    private SpawnDataStore spawnDataStore;
    private HashMap<String, List<String>> alias2jobs;

    private final AvailableCache<List<String>> mapCache;
    private final List<String> jobs = new ArrayList<>();

    // System.out.println("sJobs = " +  sJobs);  // sJobs = ["job11","job12"]
    public AliasCache() throws Exception {
        spawnDataStore = DataStoreUtil.makeCanonicalSpawnDataStore();

        mapCache = new AvailableCache<List<String>>(DEFAULT_REFRESH_INTERVAL, DEFAULT_CACHE_EXPIRE, DEFAULT_CACHE_SIZE, 2) {

            @Override public List<String> fetchValue(String id) {

                String child;
                try {
                    child = spawnDataStore.getChild(ALIAS_PATH, id);
                    if(child == null) {
                        return null;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }

                try {
                    String sJobs = getJobsFromDatastore(id, spawnDataStore.getChild(ALIAS_PATH, id));
                    ObjectMapper mapper = new ObjectMapper();

                    if(Strings.isNullOrEmpty(sJobs.toString())) {
                        System.out.println("sJobs is empty !");
                        return null;
                    } else {
                        System.out.println("sJobs in cache = " + sJobs);
                    }

                    List<String> jobs = mapper.readValue(sJobs.toString(), new TypeReference<List<String>>() {});
                    return jobs;
                } catch (Exception e) {
                    log.warn("Exception when fetching alias: {}", id, e);
                    try {
                        return null;
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                }
                return null;
            }
        };
   }

   public List<String> getJobs(String alias) throws ExecutionException {
       mapCache.cleanUp();


       List<String> jobs = mapCache.get(alias);
        if(jobs == null || jobs.size() == 0) {
            System.out.println("not jobs for alias " + alias);
            return null;
        } else {
            System.out.println("AliasCache getJobs =  " + alias);
            return jobs;
        }
   }

   public void deleteAlias(String alias) {
        mapCache.remove(alias);
        mapCache.cleanUp();
    }

    @Nullable public String getJobsFromDatastore(String alias, @Nullable String data) throws Exception {
        if (alias == null) {
            return data;
        }
        if ((data == null) || data.isEmpty()) {
            mapCache.remove(alias);
            return data;
        }
        String jobs = spawnDataStore.getChild(ALIAS_PATH, alias);
        if(Strings.isNullOrEmpty(jobs)) {
            System.out.println(alias + " has been deleted from datastore !!!");
            return null;
        }

        return jobs;
    }

    public AvailableCache<List<String>> getMapCache() {
        return mapCache;
    }

}