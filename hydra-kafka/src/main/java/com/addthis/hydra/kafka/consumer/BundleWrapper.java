package com.addthis.hydra.kafka.consumer;

import com.addthis.bundle.core.Bundle;

class BundleWrapper {

    static final BundleWrapper bundleQueueEndMarker = new BundleWrapper(null, null, 0);

    public final Bundle bundle;
    public final String sourceIdentifier;
    public final long offset;

    public BundleWrapper(Bundle bundle, String sourceIdentifier, long offset) {
        this.bundle = bundle;
        this.sourceIdentifier = sourceIdentifier;
        this.offset = offset;
    }
}
