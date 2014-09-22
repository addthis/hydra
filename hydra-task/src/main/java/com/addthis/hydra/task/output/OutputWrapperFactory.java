package com.addthis.hydra.task.output;

import com.addthis.codec.annotations.Pluggable;

import java.io.IOException;

/**
 * Interface for classes capable of [re]opening {@link com.addthis.hydra.task.output.OutputWrapper}s.
 *
 */
@Pluggable("output-factory")
public interface OutputWrapperFactory {

    /**
     * Open a new or reopen an existing {@link com.addthis.hydra.task.output.OutputWrapper}
     * and return a reference to that object
     *
     * @param target the raw target name for the output stream
     * @param outputFlags {@link com.addthis.hydra.task.output.OutputStreamFlags} for controlling output behavior
     * @param streamEmitter emitter to convert {@link com.addthis.bundle.core.Bundle}s into bytes
     * @return a reference to a {@link com.addthis.hydra.task.output.OutputWrapper} instance
     * @throws IOException
     */
    OutputWrapper openWriteStream(String target,
                                  OutputStreamFlags outputFlags,
                                  OutputStreamEmitter streamEmitter) throws IOException;
}
