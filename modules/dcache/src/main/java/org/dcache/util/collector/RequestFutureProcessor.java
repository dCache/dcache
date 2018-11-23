/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.util.collector;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.TimeoutCacheException;
import dmg.cells.nucleus.NoRouteToCellException;

/**
 * <p>Used in conjunction with the {@link CellMessagingCollector} as message
 * post-processor.  Updates the data based on the info received,
 * then does any needed post-processing.  Post-processing occurs after
 * a barrier (when all futures have been processed and removed from the
 * future map).</p>
 */
public abstract class RequestFutureProcessor<T extends Serializable, D> {
    protected static final Logger LOGGER
                    = LoggerFactory.getLogger(RequestFutureProcessor.class);

    protected final Map<String, ListenableFutureWrapper<D>> futureMap = new HashMap<>();

    protected final Map<String, T> next = new ConcurrentHashMap<>();

    protected Executor executor;

    /**
     * <p>Cancels all futures in map.</p>
     */
    public synchronized void cancel() {
        futureMap.values().stream()
                 .forEach(wrapper -> wrapper.getFuture().cancel(true));
        futureMap.clear();
    }

    /**
     * <p>Main routine sets up a barrier on listener tasks so that it can
     * run the post-process after all updates have completed.
     * Adds listener to each future returned.</p>
     *
     * @param futureMap returned from most recent collection
     * @throws IllegalStateException if the processor is still
     *                               running a previous pass
     */
    public synchronized void process(Map<String, ListenableFutureWrapper<D>> futureMap)
                    throws IllegalStateException, IllegalArgumentException {
        if (!this.futureMap.isEmpty()) {
            String error = "Cannot execute process; previous processing "
                            + "has not completed.";
            throw new IllegalStateException(error);
        }

        /*
         * Avoid potential concurrent modification exception when
         * removing, possibly while still adding listeners (below).
         */
        this.futureMap.clear();
        this.futureMap.putAll(futureMap);

        /*
         * Start the thread waiting for empty map first.
         * It is assumed the executor is not a singleton.
         */
        executor.execute(this::waitUntilFinished);

        /*
         * Inject a listener into the futures.
         */
        futureMap.entrySet().stream().forEach(this::addListener);
    }

    @Required
    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    /**
     * <p>Method is understood to be synchronous/single-threaded.</p>
     */
    protected abstract void postProcess();

    /**
     * <p>Should not throw anything but RuntimeException.</p>
     *
     * @param key  of the data in the cache
     * @param data to be handled and then stored in the cache
     * @param sent time the message was sent to the endpoint
     * @return object resulting from the transformation
     */
    protected abstract T process(String key, D data, long sent);

    /**
     * <p>Adds listener which is responsible for transforming the data
     * into the stored object.</p>
     *
     * @param entry holding the listenable future for the data message.
     */
    private void addListener(Map.Entry<String, ListenableFutureWrapper<D>> entry) {
        entry.getValue().getFuture().addListener(() -> {
            String key = entry.getKey();
            ListenableFutureWrapper<D> wrapper = entry.getValue();
            D received = null;
            Throwable thrownDuringExecution = null;

            try {
                received = wrapper.getFuture().get();
            } catch (InterruptedException e) {
                LOGGER.trace("Listener runnable was interrupted; returning ...");
            } catch (ExecutionException e) {
                thrownDuringExecution = e.getCause();
                if (thrownDuringExecution instanceof NoRouteToCellException) {
                    LOGGER.trace("Endpoint currently unavailable: {}.", key);
                } else if (thrownDuringExecution instanceof TimeoutCacheException ) {
                    LOGGER.trace("Request timed out for {}.", key);
                } else {
                    Throwable t = thrownDuringExecution.getCause();
                    LOGGER.warn("Update of data for {} failed: {} / {}.",
                                key,
                                thrownDuringExecution.getMessage(),
                                t == null ? "" : t.toString());
                }
            }

            if (received != null) {
                T toStore = process(key, received, wrapper.getSent());
                if (toStore != null) {
                    next.put(key, toStore);
                }
            }

            remove(key);

            if (thrownDuringExecution != null) {
                Throwables.throwIfUnchecked(thrownDuringExecution);
            }
        }, executor);
    }

    /**
     * <p>Notifies on removal.</p>
     */
    private synchronized void remove(String key) {
        futureMap.remove(key);
        LOGGER.trace("{} removed {} from future map, {} left.",
                     this.getClass().getSimpleName(), key, futureMap.size());
        notifyAll();
    }

    /**
     * <p>Waits for future map to be empty, then runs post-processing,
     * clearing the map afterwards.</po>
     */
    private void waitUntilFinished() {
        synchronized (this) {
            while (!futureMap.isEmpty()) {
                try {
                    wait(TimeUnit.SECONDS.toMillis(5));
                } catch (InterruptedException ie) {
                    LOGGER.trace("waitUntilFinished was interrupted; returning ...");
                    return;
                }
            }
        }

        postProcess();
        next.clear();
    }
}
