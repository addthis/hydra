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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AuthenticationManagerNestedTest {

    private static StaticUser user1 = new StaticUser("user1", ImmutableList.of("group2"), "unused", null, false);
    private static StaticUser user2 = new StaticUser("user2", null, "password2", null, false);
    private static StaticUser user3 = new StaticUser("user1", ImmutableList.of("group1"), "password1", null, false);

    private static List<StaticUser> innerUsers = ImmutableList.of(user3);
    private static List<StaticUser> outerUsers = ImmutableList.of(user1, user2);
    private static AuthenticationManagerStatic inner = new AuthenticationManagerStatic(innerUsers, ImmutableList.of(),
                                                                                       ImmutableList.of("user1"),
                                                                                       ImmutableList.of(), false);
    private static AuthenticationManagerStatic outer = new AuthenticationManagerStatic(outerUsers, ImmutableList.of(),
                                                                                       ImmutableList.of("user2"),
                                                                                       ImmutableList.of(), false);
    private static AuthenticationManagerNested auth = new AuthenticationManagerNested(inner, outer);
    @Test
    public void authentication() {
        assertEquals("password1", auth.login("user1", "password1", false));
        assertEquals(ImmutableList.of("group1", "group2"), auth.authenticate("user1", "password1").groups());
        assertEquals("password2", auth.login("user2", "password2", false));
    }

    @Test
    public void isAdmin(){
        assertTrue(auth.isAdmin(auth.authenticate("user1","password1")));
        assertTrue(auth.isAdmin(auth.authenticate("user2","password2")));
        assertFalse(auth.isAdmin(auth.authenticate("user3","password3")));
    }
}
