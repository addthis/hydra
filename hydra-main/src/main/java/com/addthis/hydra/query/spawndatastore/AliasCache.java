package com.addthis.hydra.query.spawndatastore;

import java.io.IOException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.job.store.AvailableCache;
import com.addthis.hydra.job.store.DataStoreUtil;
import com.addthis.hydra.job.store.SpawnDataStore;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AliasCache {
    private static final Logger log = LoggerFactory.getLogger(AliasCache.class);

    /* The interval to refresh cached alias values */
    private static long DEFAULT_REFRESH_INTERVAL = Parameter.longValue("alias.bimap.refresh", 1000);
    /* The expiration period for cache values. Off by default, but useful for testing. */
    private static long DEFAULT_CACHE_EXPIRE = Parameter.longValue("alias.bimap.expire", -1);
    /* The max size of the alias cache */
    private static int DEFAULT_CACHE_SIZE = Parameter.intValue("alias.bimap.cache.size", 1000);
    private static final long maintenanceInterval = 1000;

    private static final String ALIAS_PATH = "/query/alias";
    private final SpawnDataStore spawnDataStore;
    private AvailableCache<List<String>> mapCache;

    public AliasCache() throws Exception {
        spawnDataStore = DataStoreUtil.makeCanonicalSpawnDataStore();
        mapCache = new AvailableCache<List<String>>(DEFAULT_REFRESH_INTERVAL, DEFAULT_CACHE_EXPIRE, DEFAULT_CACHE_SIZE, 2) {
            @Override public List<String> fetchValue(String alias) {
                String jobs;
                try {
                    jobs = spawnDataStore.getChild(ALIAS_PATH, alias);
                    if(Strings.isNullOrEmpty(jobs)) {
                        log.error("There is no jobs for alias {}", alias);
                        return null;
                    } else {
                        return new ObjectMapper().readValue(jobs, new TypeReference<List<String>>() {});
                    }
                } catch (Exception e) {
                    log.error("Error occurred while getting alias {} from Spawn datastore", alias, e);
                    return null;
                }
            }
        };
        maybeInitMaintenance();
    }

    public void loadCurrentValues() throws IOException {
        Map<String, String> aliases = spawnDataStore.getAllChildren(ALIAS_PATH);
        if ((aliases == null) || aliases.isEmpty()) {
            log.warn("No aliases found, unless this is on first cluster startup something is probably wrong");
            return;
        }
        mapCache.clear();
        ObjectMapper mapper = new ObjectMapper();
        for (Map.Entry<String, String> aliasEntry : aliases.entrySet()) {
            List<String> jobs = mapper.readValue(aliasEntry.getValue(), new TypeReference<List<String>>() {});
            mapCache.put(aliasEntry.getKey(), jobs);
        }
    }

    private void maybeInitMaintenance() {
        if (maintenanceInterval > 0) {
            aliasCacheMaintainer.scheduleAtFixedRate(() -> {
                mapCache.cleanUp();
                mapCache.getLoadingCache().asMap().keySet().forEach(mapCache.getLoadingCache()::getIfPresent);
            }, maintenanceInterval, maintenanceInterval, TimeUnit.MILLISECONDS);
        }
    }

    private final ScheduledExecutorService aliasCacheMaintainer = MoreExecutors.getExitingScheduledExecutorService(
            new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder().setNameFormat("aliasCacheMaintainer=%d").build()));

    public List<String> getJobs(String alias) throws ExecutionException {
        List<String> jobs = mapCache.get(alias);
        if(jobs == null || jobs.size() == 0) {
            log.error("There is no job for alias {} ", alias);
            return null;
        }
        return jobs;
    }

   public void deleteAlias(String alias) {
        mapCache.remove(alias);
    }
}