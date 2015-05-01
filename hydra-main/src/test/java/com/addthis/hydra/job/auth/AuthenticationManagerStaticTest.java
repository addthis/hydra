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

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AuthenticationManagerStaticTest {

    private static StaticUser user1 = new StaticUser("user1", ImmutableList.of("group2"), null, null);
    private static StaticUser user2 = new StaticUser("user2", null, "password2", null);
    private static StaticUser user3 = new StaticUser("user1", ImmutableList.of("group1"), "password1", null);

    private static List<StaticUser> users1 = ImmutableList.of(user1, user2);
    private static List<StaticUser> users2 = ImmutableList.of(user3);

    @Test
    public void oneLevelAuth() {
        AuthenticationManagerStatic auth = new AuthenticationManagerStatic(users1, null, null, null);
        assertNull(auth.login("user1", "bar"));
        assertEquals("password2", auth.login("user2", "password2"));
        assertNull(auth.authenticate("user1", "bar"));
        assertNotNull(auth.authenticate("user2", "password2"));
    }

    @Test
    public void twoLevelAuth() {
        AuthenticationManagerStatic auth2 = new AuthenticationManagerStatic(users2, null, null, null);
        AuthenticationManagerStatic auth1 = new AuthenticationManagerStatic(users1, null, null, auth2);
        assertEquals("password1", auth1.login("user1", "password1"));
        assertEquals(ImmutableList.of("group1", "group2"), auth1.authenticate("user1", "password1").groups());
    }
}
