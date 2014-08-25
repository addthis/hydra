package com.addthis.hydra.job.alias;

import java.util.List;
import java.util.Map;

public interface AliasManager {

    Map<String, List<String>> getAliases();

    void addAlias(String alias, List<String> jobs);

    void deleteAlias(String alias);

    List<String> aliasToJobs(String alias);

}
