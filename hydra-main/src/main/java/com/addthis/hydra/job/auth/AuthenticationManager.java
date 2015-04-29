package com.addthis.hydra.job.auth;

import com.google.common.collect.ImmutableList;

public interface AuthenticationManager {

    /**
     * Returns a non-null secret token if authentication
     * was successful. Or null if authentication failed.
     *
     * @param username
     * @param password
     * @return non-null secret if authentication succeeded
     */
    String login(String username, String password);

    User authenticate(String username, String secret);

    void logout(User user);

    ImmutableList<String> adminGroups();

    ImmutableList<String> adminUsers();

}
