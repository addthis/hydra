package com.addthis.hydra.data.query.op;

import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.query.AbstractQueryOp;

import io.netty.channel.ChannelProgressivePromise;

public class OpForward extends AbstractQueryOp {

    protected OpForward(ChannelProgressivePromise opPromise) {
        super(opPromise);
    }

    @Override
    public void send(Bundle bundle) {
        getNext().send(bundle);
    }

    @Override
    public void sendComplete() {
        // sendComplete messages suppressed by this operation
    }
}
