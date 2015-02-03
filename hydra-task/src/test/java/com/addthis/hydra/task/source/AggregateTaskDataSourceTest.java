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
package com.addthis.hydra.task.source;

import org.junit.Test;
import org.mockito.Mockito;


public class AggregateTaskDataSourceTest {

    @Test
    public void testOpen() throws Exception {
        AggregateTaskDataSource aggregateTaskDataSource = new AggregateTaskDataSource();
        TaskDataSource mockDS1 = Mockito.mock(TaskDataSource.class);
        TaskDataSource mockDS2 = Mockito.mock(TaskDataSource.class);
        aggregateTaskDataSource.setSources(new TaskDataSource[]{mockDS1, mockDS2});

        mockDS1.init();
        mockDS2.init();

        Mockito.when(mockDS1.isEnabled()).thenReturn(true);
        Mockito.when(mockDS2.isEnabled()).thenReturn(true);

        Mockito.when(mockDS1.peek()).thenReturn(null);
        Mockito.when(mockDS2.peek()).thenReturn(null);

        aggregateTaskDataSource.init();
        Mockito.verify(mockDS1).peek();
        Mockito.verify(mockDS2).peek();
    }

    @Test
    public void testPeek() throws Exception {
        AggregateTaskDataSource aggregateTaskDataSource = new AggregateTaskDataSource();
        TaskDataSource mockDS1 = Mockito.mock(TaskDataSource.class);
        TaskDataSource mockDS2 = Mockito.mock(TaskDataSource.class);
        aggregateTaskDataSource.setSources(new TaskDataSource[]{mockDS1, mockDS2});

        mockDS1.init();
        mockDS2.init();

        Mockito.when(mockDS1.isEnabled()).thenReturn(true);
        Mockito.when(mockDS2.isEnabled()).thenReturn(true);

        Mockito.when(mockDS1.peek()).thenReturn(null);
        Mockito.when(mockDS2.peek()).thenReturn(null);

        aggregateTaskDataSource.init();
        aggregateTaskDataSource.peek();
        Mockito.verify(mockDS1).peek();
        Mockito.verify(mockDS2).peek();
    }
}
