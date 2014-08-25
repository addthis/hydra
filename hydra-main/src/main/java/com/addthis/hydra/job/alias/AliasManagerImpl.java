package com.addthis.hydra.job.alias;

import java.util.List;
import java.util.Map;

import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.hydra.query.spawndatastore.AliasBiMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AliasManagerImpl implements AliasManager {

    private static final Logger log = LoggerFactory.getLogger(AliasManagerImpl.class);

    private AliasBiMap aliasBiMap;

    public AliasManagerImpl(SpawnDataStore spawnDataStore) {
        this.aliasBiMap = new AliasBiMap(spawnDataStore);
        aliasBiMap.loadCurrentValues();
    }

    /**
     * Returns a map describing alias name => jobIds
     */
    public Map<String, List<String>> getAliases() {
        return aliasBiMap.viewAliasMap();
    }

    public List<String> aliasToJobs(String alias) {
        return aliasBiMap.getJobs(alias);
    }

    /**
     * Updates the full job id list of an alias.
     * 
     * This method does nothing if the give job id list is empty.
     */
    public void addAlias(String alias, List<String> jobs) {
        if (jobs.size() > 0) {
            aliasBiMap.putAlias(alias, jobs);
        } else {
            log.warn("Ignoring empty jobs addition for alias: {}", alias);
        }
    }

    public void deleteAlias(String alias) {
        aliasBiMap.deleteAlias(alias);
    }

}
