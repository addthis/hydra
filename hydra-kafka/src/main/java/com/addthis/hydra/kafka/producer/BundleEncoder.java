package com.addthis.hydra.kafka.producer;

import java.io.IOException;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.io.DataChannelCodec;

import kafka.serializer.Encoder;

/**
 * Created by steve on 2/3/15.
 */
public class BundleEncoder implements Encoder<Bundle> {

    @Override
    public byte[] toBytes(Bundle bundle) {
        try {
            return DataChannelCodec.encodeBundle(bundle);
        } catch (IOException e) {
            //this exception is never actually thrown
            return null;
        }
    }
}
