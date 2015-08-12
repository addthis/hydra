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
package com.addthis.hydra.task.source;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;

import java.nio.channels.FileLock;

import com.addthis.basis.util.LessFiles;
import com.addthis.basis.util.Parameter;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.PageDB;
import com.addthis.hydra.store.nonconcurrent.NonConcurrentPage;
import com.addthis.hydra.store.nonconcurrent.NonConcurrentPageCache;
import com.addthis.hydra.store.skiplist.ConcurrentPage;
import com.addthis.meshy.service.stream.StreamService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * tracks and auto-indexes (when possible) SourceTypeStateful sources
 */
public class SourceTracker {

    private static final Logger log = LoggerFactory.getLogger(SourceTracker.class);
    private final boolean ignoreBundleCorruption = Parameter.boolValue("sourceTracker.ignoreBundleCorruption", true);

    private final PageDB<SimpleMark> db;
    private final FileLock lockDir;

    public SourceTracker(String dir) {
        File dirFile = LessFiles.initDirectory(dir);
        try {
            lockDir = new RandomAccessFile(new File(dirFile, "tracker.lock"), "rw").getChannel().lock();
            db = new PageDB<SimpleMark>(dirFile, SimpleMark.class, 100, 100, ConcurrentPage.ConcurrentPageFactory.singleton);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * tracker MUST be closed when finished or data could be lost.
     */
    public void close() {
        db.close();
        try {
            lockDir.release();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void open(final TaskDataSource source) {
        source.init(true);
    }

    /**
     * return a wrapped source that saves state automatically on close().
     * failure to close() will result in a loss of tracking state.  close()
     * can be called on a wrapped/tracked source even if next() fails.
     * <p/>
     * this method is responsible for init()ing the source because
     * some sources may be skipped and not opened.  closing a source that
     * didn't need to be opened is wasteful.
     * <p/>
     * NOTE:  the source must be init before calling this method
     *
     * @param source source to index and track
     * @return wrapped source or null if it could not be tracked
     */
    public TaskDataSource init(final TaskDataSource source) {
        if (!(source instanceof SourceTypeStateful)) {
            return null;
        }
        final SourceTypeStateful stateful = (SourceTypeStateful) source;
        if (initStateful(stateful)) {
            if (log.isDebugEnabled()) log.debug("[init] wrapping " + source + " // " + source.getClass().getSimpleName());
            return new AbstractDataSourceWrapper(source) {
                private long read = 0;
                private boolean end = false;

                @Override
                public Bundle next() throws DataChannelError {
                    Bundle next = null;
                    try {
                        if (log.isTraceEnabled()) log.trace("ADSW.next(" + getKey(stateful) + ")");
                        next = super.next();
                        if (log.isTraceEnabled()) log.trace("ADSW.next(" + getKey(stateful) + ")=" + next);
                    } catch (Exception ex) {
                        if (log.isTraceEnabled()) log.trace("ADSW.next.err(" + getKey(stateful) + ") = " + ex);
                        // We have made no progress and have tried before, give up forever
                        return handleSourceError(read, end, stateful, ex);
                    }
                    if (next == null) {
                        end = true;
                    }
                    read++;
                    return next;
                }

                @Override
                public Bundle peek() throws DataChannelError {
                    Bundle peek = null;
                    try {
                        if (log.isTraceEnabled()) log.trace("ADSW.peek(" + getKey(stateful) + ")");
                        peek = super.peek();
                        if (log.isTraceEnabled()) log.trace("ADSW.peek(" + getKey(stateful) + ")=" + peek);
                    } catch (Exception ex) {
                        if (log.isTraceEnabled()) log.trace("ADSW.peek.err(" + getKey(stateful) + ") = " + ex);
                        // We have made no progress and have tried before, give up forever
                        return handleSourceError(read, end, stateful, ex);
                    }
                    if (peek == null) {
                        end = true;
                    }
                    return peek;
                }

                @Override
                public void close() {
                    checkpoint(stateful, read, end);
                    super.close();
                }
            };
        }
        return null;
    }

    /**
     * Opens and initializes a stateful data source
     * <p/>
     * return a wrapped source that saves state automatically on close().
     * failure to close() will result in a loss of tracking state.  close()
     * can be called on a wrapped/tracked source even if next() fails.
     * <p/>
     * this method is responsible for init()ing the source because
     * some sources may be skipped and not opened.  closing a source that
     * didn't need to be opened is wasteful.
     *
     * @param source source to index and track
     * @return wrapped source or null if it could not be tracked
     */
    public TaskDataSource openAndInit(final TaskDataSource source) {
        open(source);
        return init(source);
    }

    private Bundle handleSourceError(long read, boolean end, SourceTypeStateful stateful, Exception ex) {
        // on shutdown rejected execution may be through, we shouldn't abandon a source because of that
        // or because the socket to the streamserver/kafka stalled
        String exToString = ex.toString();
        if (read == 0
            && !isNew(stateful)
            && !(ex instanceof RuntimeException)
            && !(ex instanceof InterruptedIOException)
            && !(exToString.contains(StreamService.ERROR_EXCEED_OPEN)) // guard against overloaded mesh
            && !(exToString.contains(StreamService.ERROR_CHANNEL_LOST)) // guard against downed mesh
            && !(exToString.contains("java.util.concurrent.RejectedExecutionException"))
            && !(ignoreBundleCorruption && ex instanceof IOException && exToString.contains("!= valid Bundle type"))
                ) {
            log.warn("[error] declaring stream " + stateful.getSourceIdentifier() + " dead to us forever because of..." + ex, ex);
            end = true;
            checkpoint(stateful, read, end);
            return null;
        }
        // otherwise just give up for now
        else {
            log.warn("[error] terminate/end stream " + stateful.getSourceIdentifier() + " on ", ex);
            checkpoint(stateful, read, false);
            return null;
        }
    }

    public boolean hasChanged(SourceTypeStateful source) {
        String key = getKey(source);
        String val = source.getSourceStateMarker();
        SimpleMark rec = getRecord(key);
        if (rec != null) {
            if (val.equals(rec.getValue()) && rec.isEnd()) {
                return false;
            }
        }
        return true;
    }

    private String getKey(SourceTypeStateful source) {
        String key = source.getSourceIdentifier();
        if (log.isTraceEnabled()) {
            log.trace("original key: " + key);
        }
        /*-------------------  HACK - SHOULD BE REMOVED WHEN WE SWITCH TO MESH FULLY ------------------------*/
        if (key.contains("/live/../gold/")) {
            key = key.replace("/live/../gold/", "/gold/");
        }
        /*------------------  END HACK ---------------------------------------------------------------------*/
        if (log.isTraceEnabled()) {
            log.trace("returning key: " + key);
        }
        return key;
    }


    /**
     * Have we ever saved information about this source before?
     */
    private boolean isNew(SourceTypeStateful source) {
        String key = getKey(source);
        SimpleMark rec = getRecord(key);
        return rec == null;
    }


    /**
     * check source against records in tracker and auto-index source to last position.
     *
     * @param source source to track
     * @return true if init was successful and source was set to last position.  false is index failed or source is up to date.
     */
    public boolean initStateful(SourceTypeStateful source) {
        try {
            String key = getKey(source);
            String val = source.getSourceStateMarker();

            SimpleMark rec = getRecord(key);
            if (rec == null) {
                if (log.isDebugEnabled()) log.debug("new " + key);
                return true;
            }
            if (!val.equals(rec.getValue()) || !rec.isEnd()) {
                if (source instanceof SourceTypeIndexable && ((SourceTypeIndexable) source).setOffset(rec.getIndex())) {
                    if (log.isDebugEnabled()) log.debug("restore " + key + " skipped " + rec.getIndex());
                    return true;
                } else {
                    long count = rec.getIndex();
                    while (count > 0) {
                        source.next();
                        count--;
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("restore " + key + " indexed " + rec.getIndex() + " missed " + count + " end " + rec.isEnd());
                    }
                    return count == 0;
                }
            } else {
                if (log.isDebugEnabled()) log.debug("unchanged " + key);
                return false;
            }
        } catch (Exception ex) {
            handleSourceError(0, false, source, ex);
            return false;
        }
    }

    private SimpleMark getRecord(String key) {
        try {
            SimpleMark rec = db.get(new DBKey(0, key));
            if (rec == null) {
                return null;
            }
            return rec;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * save position of source in tracker so it can later be re-initialized.
     *
     * @param source source to track
     * @param read   number of records read/consumed since last init()
     */
    public void checkpoint(final SourceTypeStateful source, final long read, final boolean end) {
        try {
            String key = getKey(source);
            SimpleMark rec = getRecord(key);
            if (rec == null) {
                rec = new SimpleMark();
            }
            rec.setValue(source.getSourceStateMarker());
            if (source instanceof SourceTypeIndexable) {
                rec.setIndex(((SourceTypeIndexable) source).getOffset());
            } else {
                rec.setIndex(rec.getIndex() + read);
            }
            rec.setEnd(end);
            if (log.isDebugEnabled()) {
                log.debug("save " + key + " @ " + rec.getIndex() + " [read=" + read + "] " + (end ? " [end]" : " [more]"));
            }
            db.put(new DBKey(0, key), rec);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

}
