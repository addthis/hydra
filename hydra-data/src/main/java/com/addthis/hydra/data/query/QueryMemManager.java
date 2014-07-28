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
package com.addthis.hydra.data.query;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.addthis.basis.util.MemoryCounter;

import com.addthis.bundle.core.Bundle;

/**
 * The memory manager attempts to bound all in-flight queries to a range
 * of memory usage.  It does this by handing out QueryMemTrackers upon
 * request.  These requests block until sufficient resources are available.
 * <p/>
 * Hitting soft limits starts blocking allocations and tracks.  Hitting
 * hard limits causes runtime/query exceptions to be thrown forcing the
 * cleanup of resources.
 */
public class QueryMemManager {

    private final AtomicLong usedMemory = new AtomicLong();
    private final AtomicReference<QMTracker> winner = new AtomicReference<>();
    private final long maxMemHard;
    private final long maxMemSoft;

    public QueryMemManager(long hardMaxMem, long softMaxMem) {
        this.maxMemHard = hardMaxMem;
        this.maxMemSoft = softMaxMem;
    }

    public QueryMemTracker allocateTracker() {
        if (usedMemory.get() > maxMemHard) {
            throw new QueryException("max query memory exceeded");
        }
        while (usedMemory.get() > maxMemSoft) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                QueryException.promote(e);
            }
        }
        return new QMTracker();
    }

    private final class QMTracker implements QueryMemTracker {

        private long mem;
        private int bundles;

        @Override
        protected void finalize() {
            untrackAllBundles();
        }

        /**
         * in theory this could block to prevent a race between
         * queries to consume all memory.  however, 2+ fast consumers
         * could bind up and prevent each other from completing, so
         * the system would have to be clever enough to halt all but
         * one query until the resource situation was mitigated.
         * one strategy could be to anoint the first one to hit this
         * situation as the "winner", and all subsequent ones "losers"
         * until memory was back within bounds.
         */
        @Override
        public void trackBundle(Bundle bundle) {
            bundles++;
            long memest = MemoryCounter.estimateSize(bundle);
            mem += memest;
            long used = usedMemory.addAndGet(memest);
            if (used > maxMemHard) {
                if (winner.compareAndSet(this, null)) {
                    // clean 'winner' on exception if applicable
                }
                throw new QueryException("max query memory exceeded");
            }
            /**
             * the first tracker to hit the soft limit becomes the winner
             * and all other trackers then block until the mem falls below
             * the soft limit.
             */
            while (used > maxMemSoft && !winner.compareAndSet(null, this)) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    if (winner.compareAndSet(this, null)) {
                        // clean 'winner' on exception if applicable
                    }
                    QueryException.promote(e);
                }
                used = usedMemory.addAndGet(memest);
            }
        }

        @Override
        public void untrackBundle(Bundle bundle) {
            bundles--;
            long memest = MemoryCounter.estimateSize(bundle);
            mem -= memest;
            long used = usedMemory.addAndGet(-memest);
            if (used < maxMemSoft && winner.get() == this) {
                winner.set(null);
            }
        }

        @Override
        public void untrackAllBundles() {
            long used = usedMemory.addAndGet(-mem);
            mem = 0;
            bundles = 0;
            if (used < maxMemSoft) {
                winner.set(null);
            }
        }
    }
}
