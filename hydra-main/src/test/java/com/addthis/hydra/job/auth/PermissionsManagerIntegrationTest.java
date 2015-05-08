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
package com.addthis.hydra.job.auth;

import javax.annotation.Syntax;

import java.util.UUID;

import com.addthis.codec.config.Configs;
import com.addthis.hydra.job.Job;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PermissionsManagerIntegrationTest {

    /**
     * "Alice" is an ordinary user.
     * "Bob" is a user with a static sudo token.
     * "Carol" is a user in the admin group.
     * "Dan" is an admin user.
     */
    @Syntax("HOCON") private static final String config = "users: [\n" +
                                         "{\n" +
                                         "    name: alice\n" +
                                         "    secret: alicesecret\n" +
                                         "    groups: [twosyllables]\n" +
                                         "},\n" +
                                         "{\n" +
                                         "    name: bob\n" +
                                         "    secret: bobsecret\n" +
                                         "    sudo: bobsudo\n" +
                                         "    groups: [onesyllable]\n" +
                                         "},\n" +
                                         "{\n" +
                                         "    name: carol\n" +
                                         "    secret: carolsecret\n" +
                                         "    groups: [admin,twosyllables]\n" +
                                         "},\n" +
                                         "{\n" +
                                         "    name: dan\n" +
                                         "    secret: dansecret\n" +
                                         "    groups: [onesyllable]\n" +
                                         "},\n" +
                                         "]\n" +
                                         "adminUsers: [dan]\n" +
                                         "adminGroups: [admin]\n" +
                                         "requireSSL: false\n";

    private static final PermissionsManager permissions;

    static {
        AuthorizationManager authorizationManager;
        AuthenticationManager authenticationManager;
        PermissionsManager permissionsManager = null;
        try {
            authenticationManager = Configs.decodeObject(AuthenticationManagerStatic.class, config);
            authorizationManager = Configs.decodeObject(AuthorizationManagerBasic.class, "");
            permissionsManager = new PermissionsManager(authenticationManager, authorizationManager);
        } catch (Exception ex) {
            fail("Error instantiating permissions manager: " + ex.toString());
        }
        permissions = permissionsManager;
    }

    @Test
    public void login() {
        assertEquals("alicesecret", permissions.login("alice", "alicesecret", true));
        assertEquals("alicesecret", permissions.login("alice", "alicesecret", false));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void sudoUUID(String username, String password) {
        String sudo = permissions.sudo(username, password, true);
        assertNotNull(sudo);
        UUID.fromString(sudo);
        sudo = permissions.sudo(username, password, false);
        assertNotNull(sudo);
        UUID.fromString(sudo);
    }

    @Test
    public void sudo() {
        assertNull(permissions.sudo("alice", "alicesecret", true));
        assertNull(permissions.sudo("alice", "alicesecret", false));
        assertEquals("bobsudo", permissions.sudo("bob", "bobsecret", true));
        assertEquals("bobsudo", permissions.sudo("bob", "bobsecret", false));
        sudoUUID("carol", "carolsecret");
        sudoUUID("dan", "dansecret");
    }

    @Test
    public void adminAction() {
        assertFalse(permissions.adminAction("alice", "alicesecret", null));
        assertTrue(permissions.adminAction("bob", "bobsecret", "bobsudo"));
        String sudo = permissions.sudo("carol", "carolsecret", true);
        assertTrue(permissions.adminAction("carol", "carolsecret", sudo));
        sudo = permissions.sudo("dan", "dansecret", true);
        assertTrue(permissions.adminAction("dan", "dansecret", sudo));
    }

    @Test
    public void isWritable() {
        Job testJob = new Job();
        testJob.setOwner("alice");
        testJob.setGroup("twosyllables");
        testJob.setOwnerWritable(true);
        testJob.setGroupWritable(true);
        assertTrue(permissions.isWritable("alice", "alicesecret", null, testJob));
        assertFalse(permissions.isWritable("bob", "bobsecret", null, testJob));
        assertTrue(permissions.isWritable("bob", "bobsecret", "bobsudo", testJob));
        assertTrue(permissions.isWritable("carol", "carolsecret", null, testJob));
        testJob.setWorldWritable(true);
        assertTrue(permissions.isWritable("bob", "bobsecret", null, testJob));
        testJob.setWorldWritable(false);
        testJob.setOwnerWritable(false);
        testJob.setGroupWritable(false);
        assertFalse(permissions.isWritable("alice", "alicesecret", null, testJob));
        assertFalse(permissions.isWritable("bob", "bobsecret", null, testJob));
        assertTrue(permissions.isWritable("bob", "bobsecret", "bobsudo", testJob));
        assertFalse(permissions.isWritable("carol", "carolsecret", null, testJob));
    }

    @Test
    public void isExecutable() {
        Job testJob = new Job();
        testJob.setOwner("alice");
        testJob.setGroup("twosyllables");
        testJob.setOwnerExecutable(true);
        testJob.setGroupExecutable(true);
        assertTrue(permissions.isExecutable("alice", "alicesecret", null, testJob));
        assertFalse(permissions.isExecutable("bob", "bobsecret", null, testJob));
        assertTrue(permissions.isExecutable("bob", "bobsecret", "bobsudo", testJob));
        assertTrue(permissions.isExecutable("carol", "carolsecret", null, testJob));
        testJob.setWorldExecutable(true);
        assertTrue(permissions.isExecutable("bob", "bobsecret", null, testJob));
        testJob.setWorldExecutable(false);
        testJob.setOwnerExecutable(false);
        testJob.setGroupExecutable(false);
        assertFalse(permissions.isExecutable("alice", "alicesecret", null, testJob));
        assertFalse(permissions.isExecutable("bob", "bobsecret", null, testJob));
        assertTrue(permissions.isExecutable("bob", "bobsecret", "bobsudo", testJob));
        assertFalse(permissions.isExecutable("carol", "carolsecret", null, testJob));
    }

    @Test
    public void canModifyPermissions() {
        Job testJob = new Job();
        testJob.setOwner("alice");
        testJob.setGroup("twosyllables");
        assertTrue(permissions.canModifyPermissions("alice", "alicesecret", null, testJob));
        assertFalse(permissions.canModifyPermissions("bob", "bobsecret", null, testJob));
        assertTrue(permissions.canModifyPermissions("bob", "bobsecret", "bobsudo", testJob));
        assertTrue(permissions.canModifyPermissions("carol", "carolsecret", null, testJob));
        testJob.setWorldWritable(true);
        assertTrue(permissions.canModifyPermissions("bob", "bobsecret", null, testJob));
        testJob.setWorldWritable(false);
        testJob.setOwnerWritable(false);
        testJob.setGroupWritable(false);
        assertTrue(permissions.canModifyPermissions("alice", "alicesecret", null, testJob));
        assertFalse(permissions.canModifyPermissions("bob", "bobsecret", null, testJob));
        assertTrue(permissions.canModifyPermissions("bob", "bobsecret", "bobsudo", testJob));
        assertTrue(permissions.canModifyPermissions("carol", "carolsecret", null, testJob));
    }

    @Test
    public void logout() {
        String sudo1 = permissions.sudo("carol", "carolsecret", true);
        permissions.logout("carol", "carolsecret");
        String sudo2 = permissions.sudo("carol", "carolsecret", true);
        assertNotEquals(sudo1, sudo2);
    }

}
