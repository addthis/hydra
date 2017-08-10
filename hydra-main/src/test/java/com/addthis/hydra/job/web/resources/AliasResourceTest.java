package com.addthis.hydra.job.web.resources;

import java.util.List;
import java.util.stream.IntStream;

import com.addthis.basis.kv.KVPairs;

import com.addthis.hydra.job.alias.AliasManager;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class AliasResourceTest {

    AliasManager aliasManager;
    private KVPairs kv;

    @Before
    public void setUp() throws Exception {
        aliasManager = mock(AliasManager.class);
        kv = new KVPairs();
    }

    /*

    query Request URL:http://adq05:2222/query/call?path=%2F(25)%2B%3A%2Bcount%2C%2Bnodes%2C%2Bmem&ops=gather%3Dksaau%3Bsort%3D0%3As%3Aa&format=json&job=8939260c-395c-4355-b5a0-3ca23c4a113c&sender=spawn&nocache=1
    path=%2F(25)%2B%3A%2Bcount%2C%2Bnodes%2C%2Bmem&ops=gather%3Dksaau%3Bsort%3D0%3As%3Aa&format=json&job=8939260c-395c-4355-b5a0-3ca23c4a113c&sender=spawn&nocache=1

    path:/(25)+:+count,+nodes,+mem
    ops:gather=ksaau;sort=0:s:a
    format:json
    job:8939260c-395c-4355-b5a0-3ca23c4a113c
    sender:spawn
    nocache:1

http://spawn-iad:5052/alias/save

name=test-alias1&jobs=8939260c-395c-4355-b5a0-3ca23c4a113c
     */

    @Test
    public void postAlias() throws Exception {
        kv.add("name", "test-alias1");
        kv.add("jobs", "8939260c-395c-4355-b5a0-3ca23c4a113c");

        List<String> jobs = Lists.newArrayList(Splitter.on(',').split(kv.getValue("jobs")));

        IntStream.rangeClosed(1, 1000).forEach(i -> {
            System.out.println(i);
            aliasManager.addAlias(kv.getValue("name"), jobs);
        });



    }

}