package com.addthis.hydra.minion;

import java.io.Closeable;
import java.io.IOException;

abstract class HostLocationInitializer implements Closeable{

    abstract HostLocation getHostLocation();

    @Override
    public void close() throws IOException {}
}
