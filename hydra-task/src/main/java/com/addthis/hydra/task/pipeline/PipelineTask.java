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
package com.addthis.hydra.task.pipeline;

import javax.annotation.Nonnull;

import java.io.IOException;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.addthis.hydra.task.map.StreamMapper;
import com.addthis.hydra.task.run.TaskRunnable;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>This is a <span class="hydra-summary">pipeline of one or more hydra jobs</span>.
 * It is specified with {@code type : "pipeline"}.</p>
 * <p>A pipeline job will run the first job phase to completion, then the second
 * phase, then the third phase, etc. When a pipeline job stops and is restarted
 * it begins processing from the first phase. It is recommended that phases use
 * the traditional mechanisms for processing data at most once, ie. the marks
 * directories.</p>
 * <p>Use the notation {@code ${hydra.task.jobid}} to retrieve the identifier
 * of the currently running job. This should be used by downstream phases to read
 * the files produced by an upstream phase.</p>
 * <p>By default error checking is enabled to verify that each job phase
 * does not write to an output directory of another job phase. If you want
 * to disable this error checking then set {@code validateDirs} to false.</p>
 * <p>All logging information is printed out using human (counting from 1)
 * numbering of the phases.</p>
 * <p>Example:</p>
 * <pre>{
 *    type : "pipeline",
 *    phases: [
 *    ]
 * }</pre>
 *
 * @user-reference
 * @hydra-name pipeline
 */
public class PipelineTask implements TaskRunnable {

    private static final Logger log = LoggerFactory.getLogger(PipelineTask.class);

    @Nonnull private final StreamMapper[] phases;

    private final ImmutableList<CompletableFuture<Void>> phaseComplete;

    private final ImmutableList<CompletableFuture<Void>> phaseNext;

    private volatile StreamMapper currentPhase = null;

    @JsonCreator
    public PipelineTask(@JsonProperty("phases") @Nonnull StreamMapper[] phases) {
        this.phases = phases;
        int futures = Math.max(phases.length - 1, 0);
        ImmutableList.Builder<CompletableFuture<Void>> complete = new ImmutableList.Builder<>();
        ImmutableList.Builder<CompletableFuture<Void>> next = new ImmutableList.Builder<>();
        for (int i = 0; i < futures; i++) {
            final int current = i;
            CompletableFuture<Void> phaseCompleteFuture = phases[i].getCompletionFuture();
            CompletableFuture<Void> phaseNextFuture = phaseCompleteFuture.thenRun(() -> beginPhase(current + 1));
            complete.add(phaseCompleteFuture);
            next.add(phaseNextFuture);
        }
        this.phaseComplete = complete.build();
        this.phaseNext = next.build();
    }

    @Override public void start() {
        beginPhase(0);
    }

    @Override public void close() throws Exception {
        log.info("Pipeline task is starting shutdown");
        int size = phaseComplete.size();
        boolean cancel[] = new boolean[size];
        for (int i = (size - 1); i >= 0; i--) {
            cancel[i] = phaseComplete.get(i).cancel(false);
        }
        for (int i = (size - 1); i >= 0; i--) {
            try {
                if (!cancel[i]) {
                    phaseNext.get(i).join();
                }
            } catch (CompletionException ex) {
                String msg = "Phase " + (i + 1) + " phaseNext future encountered an " +
                             "exception while starting phase " + (i + 2);
                throw new IOException(msg, ex);
            } catch (CancellationException ex) {
                String msg = "Race condition: Phase " + (i + 1) + " phaseNext " +
                             "future was cancelled by another thread";
                throw new IOException(msg, ex);
            }
        }
        /**
         * At this point all phaseNext futures have either
         * completed or have been cancelled. It is now
         * safe to close the current phase.
         */
        if (currentPhase != null) {
            currentPhase.close();
        }
    }

    /**
     * Begin a phase. Clears the {@code currentPhase} before initializing
     * the new phase and assigns {@code currentPhase} on successful
     * initialization.
     */
    private void beginPhase(int pos) {
        if (pos < phases.length) {
            log.info("Initializing phase {} for execution.", pos + 1);
            currentPhase = null;
            phases[pos].start();
            currentPhase = phases[pos];
        }
    }

}
