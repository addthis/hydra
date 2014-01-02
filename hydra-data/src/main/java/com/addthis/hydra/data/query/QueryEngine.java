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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.util.ClosableIterator;

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.kvp.KVBundleFormat;
import com.addthis.hydra.data.tree.DataTree;
import com.addthis.hydra.data.tree.DataTreeNode;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * wraps a Tree and provides the real work behind the query engine. keeps track
 * of active queries so that they can be canceled.
 */
public class QueryEngine {

    private static final Logger log = LoggerFactory.getLogger(QueryEngine.class);
    private static final LinkedList<DataTreeNode> emptylist = new LinkedList<DataTreeNode>();

    protected DataTree tree;
    private AtomicInteger used;
    private AtomicBoolean isOpen;
    private AtomicBoolean isClosed;
    private final HashSet<Thread> active;
    private boolean closeWhenIdle;

    public QueryEngine(DataTree tree) {
        this.tree = tree;
        this.used = new AtomicInteger(0);
        this.isOpen = new AtomicBoolean(false);
        this.isClosed = new AtomicBoolean(false);
        this.active = new HashSet<>();
    }

    public int getLeasesCount() {
        return used.intValue();
    }

    @Override
    public String toString() {
        return "[QueryEngine:" + tree + ":" + used + ":" + isOpen + ":" + isClosed + "]";
    }

    public boolean isclosed() {
        return isClosed.get();
    }

    public synchronized boolean lease() {
        if (isClosed.get()) {
            if (log.isWarnEnabled()) {
                log.warn("lease fail on closed for " + tree);
            }
            return false;
        }
        if (used.getAndIncrement() >= 0) {
            try {
                init();
                return true;
            } catch (Exception ex)  {
                log.warn("", ex);
            }
        }
        if (log.isWarnEnabled()) {
            log.warn("lease fail on user count " + used + " for " + tree);
        }
        used.decrementAndGet();
        return false;
    }

    /**
     */
    public synchronized void release() {
        int uv = used.decrementAndGet();
        assert (uv >= 0);
        if (uv == 0 && closeWhenIdle) {
            close();
            if (log.isDebugEnabled()) {
                log.debug("close on idle/release for " + tree);
            }
        } else if (log.isDebugEnabled()) {
            log.debug("release but not closing for " + tree);
        }
    }

    /**
     */
    public synchronized void closeWhenIdle() {
        closeWhenIdle = true;
        if (used.get() == 0) {
            close();
            if (log.isDebugEnabled()) {
                log.debug("close on idle/close for " + tree);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Query Engine " + tree + " is busy, did not call close");
            }
        }
    }

    /**
     * effectively kills current queries
     */
    public void cancelActiveThreads() {
        synchronized (active) {
            for (Thread thread : active) {
                thread.interrupt();
            }
        }
    }

    /**
     * Calls close on the tree object
     */
    public void close() {
        synchronized (this) {
            try {
                if (isClosed.compareAndSet(false, true) && isOpen.compareAndSet(true, false)) {
                    if (used.get() > 0 || active.size() > 0) {
                        log.warn("closing with leases=" + used + ", active queries=" + active.size());
                    }
                }
                cancelActiveThreads();
            } finally {
                tree.close();
            }
        }
    }

    public void init() throws QueryException {
        synchronized (this) {
            if (isClosed.get()) {
                throw new QueryException("Query Engine Closed");
            }
            isOpen.set(true);
        }
    }

    /**
     * Performs a query search, writes the results to a data channel. This function does not break the execution of the
     * query if the client channel gets closed.
     *
     * @param query  A Query object that contains the path or paths of the root query.
     * @param result A DataChannelOutput to which the result will be written. In practice, this will be the head of
     *               a QueryOpProcessor that represents the first operator in a query, which in turn sends its output
     *               to another QueryOpProcessor and the last will send its output to a DataChannelOutput sending bytes
     *               back to meshy, usually defined at the MQSource side of code.
     */
    public void search(Query query, DataChannelOutput result) throws QueryException {
        search(query, result, new QueryStatusObserver());
    }

    /**
     * Performs a query search, writes the results to a data channel. This function does not break the execution of the
     * query if the client channel gets closed.
     *
     * @param query    A Query object that contains the path or paths of the root query.
     * @param result   A DataChannelOutput to which the result will be written. In practice, this will be the head of
     *                 a QueryOpProcessor that represents the first operator in a query, which in turn sends its output
     *                 to another QueryOpProcessor and the last will send its output to a DataChannelOutput sending bytes
     *                 back to meshy, usually defined at the MQSource side of code.
     * @param observer A wrapper for a boolean flag that gets set to true by MQSource in case the user
     *                 cancels the query at the MQMaster side.
     */
    public void search(Query query, DataChannelOutput result, QueryStatusObserver observer) throws QueryException {
        for (QueryElement[] path : query.getQueryPaths()) {
            if (!(observer.queryCancelled || observer.queryCompleted)) {
                search(path, result, observer);
            }
        }
    }

    /**
     * Performs a query search, writes the results to a data channel, and stops processing if the source sets
     * queryStatusObserver.queryCancelled to true.
     *
     * TODO Currently only exists to satisfy legacy PathOutput needs.  PathOutput needs updating/deprecating.
     *
     * @param path   An array of QueryElement that contains a parsed query path.
     * @param result A DataChannelOutput to which the result will be written. In practice, this will be the head of
     *               a QueryOpProcessor that represents the first operator in a query, which in turn sends its output
     *               to another QueryOpProcessor and the last will send its output to a DataChannelOutput sending bytes
     *               back to meshy, usually defined at the MQSource side of code.
     * @throws QueryException
     */
    public void search(QueryElement path[], DataChannelOutput result) throws QueryException {
        search(path, result, new QueryStatusObserver());
    }

    /**
     * Performs a query search, writes the results to a data channel, and stops processing if the source sets
     * queryStatusObserver.queryCancelled to true.
     *
     * @param path     An array of QueryElement that contains a parsed query path.
     * @param result   A DataChannelOutput to which the result will be written. In practice, this will be the head of
     *                 a QueryOpProcessor that represents the first operator in a query, which in turn sends its output
     *                 to another QueryOpProcessor and the last will send its output to a DataChannelOutput sending bytes
     *                 back to meshy, usually defined at the MQSource side of code.
     * @param observer A wrapper for a boolean flag that gets set to true by MQSource in case the user
     *                 cancels the query at the MQMaster side.
     * @throws QueryException
     * @see {@link Query#parseQueryPath(String)}
     */
    private void search(QueryElement path[], DataChannelOutput result, QueryStatusObserver observer) throws QueryException {
        init();
        Thread thread = Thread.currentThread();
        synchronized (active) {
            if (!active.add(thread)) {
                throw new QueryException("Active Thread " + thread + " reentering search");
            }
        }
        try {
            LinkedList<DataTreeNode> stack = new LinkedList<>();
            stack.push(tree);
            tableSearch(stack, new FieldValueList(new KVBundleFormat()), path, 0, result, 0, observer);
        } catch (QueryException ex) {
            if (log.isDebugEnabled()) {
                log.debug("", ex);
            }
        } catch (RuntimeException ex)  {
            log.warn("", ex);
            throw ex;
        } finally {
            synchronized (active) {
                if (!active.remove(thread)) {
                    log.warn("Active Thread " + thread + " missing from set");
                }
            }
        }
    }

    /**
     * the real worker behind queries. this iterates over TreeNodes using
     * QueryElement definitions. This function (and the other tableSearchs it calls) will check the queryStatusObserver
     * repeatedly to make sure that the channel to the client is still up. If the channel gets closed and queryStatusObserver.queryCancelled
     * was set to true, they will break and throw QueryExceptions.
     *
     * @param stack
     * @param root
     * @param prefix
     * @param path                a parsed query path, see {@link Query#parseQueryPath(String)}
     * @param pathIndex           an integer indicating the index in the path to execute. The tableSearch functions will recursively
     *                            call themselves increasing the path until all the query paths have been executed.
     * @param result              A DataChannelOutput to write the results to, most likely the first of a chain od QueryOpProcessor(s).
     * @param collect
     * @param queryStatusObserver contains a boolean flag that gets set to true from MQSource in case the user hits
     *                            cancel at the MQMaster side. At this point, there is no need for us to continue
     *                            doing the query as the channel has been closed. Recursively, the functions will break
     *                            out by throwing QueryExceptions.
     * @throws QueryException
     */
    private void tableSearch(LinkedList<DataTreeNode> stack, DataTreeNode root, FieldValueList prefix, QueryElement path[],
            int pathIndex, DataChannelOutput result, int collect,
            QueryStatusObserver queryStatusObserver) throws QueryException {
        stack.push(root);
        tableSearch(stack, prefix, path, pathIndex, result, collect, queryStatusObserver);
        stack.pop();
    }

    /**
     * This version of table search does not take a QueryStatusObserver and so will not break if the channel
     * to the source got closed, as it will not know about it.
     *
     * @param stack
     * @param prefix
     * @param path      a parsed query path, see {@link Query#parseQueryPath(String)}
     * @param pathIndex an integer indicating the index in the path to execute. The tableSearch functions will recursively
     *                  call themselves increasing the path until all the query paths have been executed.
     * @param sink      A DataChannelOutput to write the results to, most likely the first of a chain od QueryOpProcessor(s).
     * @param collect
     * @throws QueryException
     */
    private void tableSearch(LinkedList<DataTreeNode> stack, FieldValueList prefix, QueryElement path[],
            int pathIndex, DataChannelOutput sink, int collect) throws QueryException {
        tableSearch(stack, prefix, path, pathIndex, sink, collect, new QueryStatusObserver());
    }

    /**
     * see above.
     */
    private void tableSearch(LinkedList<DataTreeNode> stack, FieldValueList prefix, QueryElement path[],
            int pathIndex, DataChannelOutput sink, int collect,
            QueryStatusObserver queryStatusObserver) throws QueryException {
        if (queryStatusObserver != null && queryStatusObserver.queryCancelled) {
            log.warn("Query closed during processing");
            throw new QueryException("Query closed during processing");
        }

        DataTreeNode root = stack != null ? stack.peek() : null;
        if (log.isDebugEnabled()) {
            log.debug("root=" + root + " pre=" + prefix + " path=" + Arrays.toString(path) + " idx=" + pathIndex + " res=" + sink + " coll=" + collect);
        }

        if (Thread.currentThread().isInterrupted()) {
            QueryException exception = new QueryException("query interrupted");
            log.warn("Query closed due to thread interruption:\n", exception);
            throw exception;
        }
        if (pathIndex >= path.length) {
            if (log.isDebugEnabled()) {
                log.debug("pathIndex>path.length, return root=" + root);
            }
            if (queryStatusObserver != null && !queryStatusObserver.queryCompleted) {
                sink.send(prefix.createBundle(sink));
            }
            return;
        }
        QueryElement next = path[pathIndex];
        Iterator<DataTreeNode> iter = root != null ? next.matchNodes(tree, stack) : next.emptyok() ? emptylist.iterator() : null;
        if (iter == null) {
            return;
        }
        try {
            int skip = next.skip();
            int limit = next.limit();
            if (next.flatten()) {
                int count = 0;
                while (iter.hasNext() && (next.limit() == 0 || limit > 0)) {
                    // Check for interruptions or cancellations
                    if (Thread.currentThread().isInterrupted()) {
                        QueryException exception = new QueryException("query interrupted");
                        log.warn("Query closed due to thread interruption:\n", exception);
                        throw exception;
                    }
                    if (queryStatusObserver != null && queryStatusObserver.queryCancelled) {
                        if (iter instanceof ClosableIterator) {
                            ((ClosableIterator<DataTreeNode>) iter).close();
                        }

                        log.warn("Query closed during processing, root=" + root);
                        throw new QueryException("Query closed during processing, root=" + root);
                    }

                    DataTreeNode tn = iter.next();
                    if (tn == null && !next.emptyok()) {
                        break;
                    }
                    if (next.hasData()) {
                        if (skip > 0) {
                            skip--;
                            continue;
                        }
                        count += next.update(prefix, tn);
                        limit--;
                    }
                }
                if (queryStatusObserver != null && !queryStatusObserver.queryCompleted) {
                    tableSearch(null, prefix, path, pathIndex + 1, sink, collect + count, queryStatusObserver);
                }
                prefix.pop(count);
                return;
            }
            while (iter.hasNext() && (next.limit() == 0 || limit > 0)) {
                // Check for interruptions or cancellations
                if (Thread.currentThread().isInterrupted()) {
                    QueryException exception = new QueryException("query interrupted");
                    log.warn("Query closed due to thread interruption:\n", exception);
                    throw exception;
                }
                if (queryStatusObserver != null && queryStatusObserver.queryCompleted) {
                    break;
                }
                if (queryStatusObserver.queryCancelled) {
                    if (iter instanceof ClosableIterator) {
                        ((ClosableIterator<DataTreeNode>) iter).close();
                    }

                    log.warn("Query closed during processing, root=" + root);
                    throw new QueryException("Query closed during processing, root=" + root);
                }

                DataTreeNode tn = iter.next();
                if (next.hasData()) {
                    if (tn == null && !next.emptyok()) {
                        return;
                    }
                    if (skip > 0) {
                        skip--;
                        continue;
                    }
                    int count = next.update(prefix, tn);
                    if (count > 0) {
                        if (!queryStatusObserver.queryCompleted) {
                            tableSearch(stack, tn, prefix, path, pathIndex + 1, sink, collect + count, queryStatusObserver);
                        }
                        prefix.pop(count);
                        limit--;
                    }
                } else {
                    if (skip > 0) {
                        skip--;
                        continue;
                    }
                    if (!queryStatusObserver.queryCompleted) {
                        tableSearch(stack, tn, prefix, path, pathIndex + 1, sink, collect, queryStatusObserver);
                    }
                    limit--;
                }
            }
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("CLOSING: root=" + root + " pre=" + prefix + " path=" + Arrays.toString(path) + " idx=" + pathIndex + " res=" + sink + " coll=" + collect);
            }

            if (iter instanceof ClosableIterator) {
                ((ClosableIterator<DataTreeNode>) iter).close();
            }
        }
    }

}
