package com.addthis.hydra.data.query;

import org.junit.Test;

public class TestOpDateFormat extends TestOp {

    @Test
    public void testOpDateFormat() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("140101").
                        tr().td("140108"),
                "datef=0:yyMMdd:yyww",
                new DataTableHelper().
                        tr().td("1401").
                        tr().td("1402")
        );
        doOpTest(
                new DataTableHelper().
                        tr().td("140101", "0").
                        tr().td("140108", "0"),
                "datef=0:yyMMdd:yyww:1",
                new DataTableHelper().
                        tr().td("140101", "1401").
                        tr().td("140108", "1402")
        );
    }
}
