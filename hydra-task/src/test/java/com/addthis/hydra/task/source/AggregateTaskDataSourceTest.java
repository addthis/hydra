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

import com.addthis.hydra.task.run.TaskRunConfig;

import org.easymock.EasyMock;
import org.junit.Test;


public class AggregateTaskDataSourceTest {

    @Test
    public void testOpen() throws Exception {
        AggregateTaskDataSource aggregateTaskDataSource = new AggregateTaskDataSource();
        TaskDataSource mockDS1 = EasyMock.createMock(TaskDataSource.class);
        TaskDataSource mockDS2 = EasyMock.createMock(TaskDataSource.class);
        aggregateTaskDataSource.setSources(new TaskDataSource[]{mockDS1, mockDS2});

        mockDS1.init(EasyMock.isA(TaskRunConfig.class));
        mockDS2.init(EasyMock.isA(TaskRunConfig.class));

        EasyMock.expect(mockDS1.isEnabled()).andReturn(true);
        EasyMock.expect(mockDS2.isEnabled()).andReturn(true);

        EasyMock.expect(mockDS1.peek()).andReturn(null);
        EasyMock.expect(mockDS2.peek()).andReturn(null);

        EasyMock.replay(mockDS1, mockDS2);
        aggregateTaskDataSource.init(new TaskRunConfig(3, 9, "foo"));
        EasyMock.verify(mockDS1, mockDS2);
    }

    @Test
    public void testPeek() throws Exception {
        AggregateTaskDataSource aggregateTaskDataSource = new AggregateTaskDataSource();
        TaskDataSource mockDS1 = EasyMock.createMock(TaskDataSource.class);
        TaskDataSource mockDS2 = EasyMock.createMock(TaskDataSource.class);
        aggregateTaskDataSource.setSources(new TaskDataSource[]{mockDS1, mockDS2});

        mockDS1.init(EasyMock.isA(TaskRunConfig.class));
        mockDS2.init(EasyMock.isA(TaskRunConfig.class));

        EasyMock.expect(mockDS1.isEnabled()).andReturn(true);
        EasyMock.expect(mockDS2.isEnabled()).andReturn(true);

        EasyMock.expect(mockDS1.peek()).andReturn(null);
        EasyMock.expect(mockDS2.peek()).andReturn(null);

        EasyMock.replay(mockDS1, mockDS2);
        aggregateTaskDataSource.init(new TaskRunConfig(3, 9, "foo"));
        aggregateTaskDataSource.peek();
        EasyMock.verify(mockDS1, mockDS2);
    }
}
