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

    @Test
    public void authentication() {
        StaticUser user1 = new StaticUser("user1", ImmutableList.of("group2"), null, null);
        StaticUser user2 = new StaticUser("user2", null, "password2", null);
        List<StaticUser> users1 = ImmutableList.of(user1, user2);
        AuthenticationManagerStatic auth = new AuthenticationManagerStatic(users1,  ImmutableList.of(),  ImmutableList.of());
        assertNull(auth.login("user1", "bar"));
        assertEquals("password2", auth.login("user2", "password2"));
        assertNull(auth.authenticate("user1", "bar"));
        assertNotNull(auth.authenticate("user2", "password2"));
    }
}
