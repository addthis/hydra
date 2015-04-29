package com.addthis.hydra.job.auth;

public interface AuthorizationManager {

    boolean isAdmin(User user);

    boolean isWritable(User user, WritableAsset asset);

}
