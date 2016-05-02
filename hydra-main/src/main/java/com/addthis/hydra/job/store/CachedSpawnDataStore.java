package com.addthis.hydra.job.store;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

public class CachedSpawnDataStore implements SpawnDataStore {
    private static Pair<String, String> defaultKey(String path) {
        return ImmutablePair.of(path, null);
    }

    private final SpawnDataStore dataStore;
    private final LoadingCache<Pair<String, String>, String> cache;

    public CachedSpawnDataStore(SpawnDataStore dataStore) {
        this.dataStore = dataStore;
        this.cache = CacheBuilder.newBuilder().build(new CacheLoader<Pair<String, String>, String>() {
            @Override
            public String load(Pair<String, String> key) throws Exception {
                String path = key.getLeft();
                String childId = key.getRight();

                if (childId == null) {
                    return CachedSpawnDataStore.this.dataStore.get(path);
                } else {
                    return CachedSpawnDataStore.this.dataStore.getChild(path, childId);
                }
            }
        });
    }

    @Override
    public String getDescription() {
        return dataStore.getDescription();
    }

    @Override
    public String get(String path) {
        try {
            return cache.get(defaultKey(path));
        } catch (CacheLoader.InvalidCacheLoadException e) {
            return null;
        } catch (ExecutionException e) {
            // TODO
            return null;
        }
    }

    @Override
    public Map<String, String> get(String[] paths) {
        List<String> notCached = new ArrayList<>();
        Map<String, String> results = new TreeMap<>();

        for (String path : paths) {
            String result = cache.getIfPresent(defaultKey(path));
            if (result == null) {
                notCached.add(path);
            } else {
                results.put(path, result);
            }
        }

        String[] remainingPaths = new String[notCached.size()];
        remainingPaths = notCached.toArray(remainingPaths);

        Map<String, String> resultsFromDB = dataStore.get(remainingPaths);

        for (Map.Entry<String, String> entry : resultsFromDB.entrySet()) {
            String path = entry.getKey();
            cache.put(defaultKey(path), entry.getValue());
        }

        results.putAll(resultsFromDB);
        return results;
    }

    @Override
    public void put(String path, String value) throws Exception {
        dataStore.put(path, value);
        cache.put(defaultKey(path), value);
    }

    @Override
    public void putAsChild(String parent, String childId, String value) throws Exception {
        dataStore.putAsChild(parent, childId, value);
        cache.put(ImmutablePair.of(parent, childId), value);
    }

    @Override
    public String getChild(String parent, String childId) throws Exception {
        return cache.get(ImmutablePair.of(parent, childId));
    }

    @Override
    public void deleteChild(String parent, String childId) {
        cache.invalidate(ImmutablePair.of(parent, childId));
        dataStore.deleteChild(parent, childId);
    }

    @Override
    public void delete(String path) {
        cache.invalidate(defaultKey(path));
        dataStore.delete(path);
    }

    @Override
    public List<String> getChildrenNames(String path) {
        return dataStore.getChildrenNames(path);
    }

    @Override
    public Map<String, String> getAllChildren(String path) {
        return dataStore.getAllChildren(path);
    }

    @Override
    public void close() {
        dataStore.close();
    }
}
