package com.addthis.hydra.task.run;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TaskRunnerTest {

    @Test
    public void testParseTaskType() {
        assertEquals(TaskRunnerType.JSON, TaskRunner.parseTaskType(""));
        assertEquals(TaskRunnerType.HOCON, TaskRunner.parseTaskType(" // input: hocon"));
        assertEquals(TaskRunnerType.HOCON, TaskRunner.parseTaskType(" # input: hocon"));
        assertEquals(TaskRunnerType.HOCON, TaskRunner.parseTaskType(" # input :hocon"));
        assertEquals(TaskRunnerType.HOCON, TaskRunner.parseTaskType("#input:com.typesafe.config"));
        assertEquals(TaskRunnerType.HOCON, TaskRunner.parseTaskType(" // -Dfoo.bar = baz \n // input: hocon"));
        assertEquals(TaskRunnerType.JSON, TaskRunner.parseTaskType(" // -Dfoo.bar = baz \n foo bar \n" +
                                                                    " // input: hocon"));
    }

}
