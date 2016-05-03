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
package org.dcache.services.billing.db.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.exceptions.RetryException;
import org.dcache.services.billing.histograms.data.IHistogramData;

/**
 * Framework for database access; uses a blocking queue and N consumer
 * threads to process requests; consumer drains the queue up to max,
 * for batching.  Commit is implemented by the store.
 *
 * @author arossi
 */
public abstract class AbstractBillingInfoAccess implements IBillingInfoAccess {
    class Consumer extends Thread {
        private Consumer(String name) {
            super(name);
        }

        public void run() {
            try {
                while (!isInterrupted()) {
                    Collection<IHistogramData> data = new ArrayList<>();

                    /*
                     * take() blocks until non-empty
                     * and throws an InterruptedException
                     */
                    logger.trace("calling queue.take()");
                    data.add(queue.take());

                    /*
                     * add to data and remove from queue any accumulated entries
                     */
                    logger.trace("calling queue.drainTo(), queue size {}",
                                 queue.size());
                    queue.drainTo(data, maxBatchSize);

                    if (isInterrupted()) {
                        break;
                    }

                    try {
                        logger.trace("calling commit");
                        commit(data);
                        committed.addAndGet(data.size());
                    } catch (RetryException t) {
                        logger.warn("commit failed; retrying once ...");
                        try {
                            commit(data);
                            committed.addAndGet(data.size());
                        } catch (RetryException t1) {
                            logger.error("commit retry failed, {} inserts have "
                                                         + "been lost",
                                         data.size());
                            logger.debug("exception in run(), commit", t1);
                        }
                    }
                }
            } catch (InterruptedException ignored) {
                logger.trace("Consumer interrupted.");
            }
        }
    }

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final AtomicLong dropped   = new AtomicLong(0);
    private final AtomicLong committed = new AtomicLong(0);

    private BlockingQueue<IHistogramData> queue;
    private List<Consumer>                consumers;
    private int                           maxQueueSize;
    private int                           maxBatchSize;
    private int                           numberOfConsumers;
    private boolean                       dropMessagesAtLimit;

    public void close() {
        if (consumers != null) {
            consumers.stream().forEach(Consumer::interrupt);
            consumers.stream().forEach((consumer) -> {
                try {
                    consumer.join();
                } catch (InterruptedException e) {
                    logger.trace("join on consumers interrupted");
                }
            });
        }
        logger.trace("{} close exiting", this);
    }

    public long getCommittedMessages() {
        return committed.get();
    }

    public long getDroppedMessages() {
        return dropped.get();
    }

    public long getInsertQueueSize() {
        return queue.size();
    }

    public void initialize() {
        logger.debug("access type: {}", this.getClass().getName());
        queue = new LinkedBlockingQueue<>(maxQueueSize);
        consumers = new ArrayList<>();
        for (int i = 0; i < numberOfConsumers; i++) {
            consumers.add(new Consumer("histogram data consumer " + i));
        }
        consumers.stream().forEach(Consumer::start);
    }

    public void put(IHistogramData data) {
        if (!dropMessagesAtLimit) {
            try {
                queue.put(data);
            } catch (InterruptedException t) {
                processInterrupted(data);
            }
        } else if (!queue.offer(data)) {
            processDroppedData(data);
        }
    }

    public void setDropMessagesAtLimit(boolean dropMessagesAtLimit) {
        this.dropMessagesAtLimit = dropMessagesAtLimit;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public void setNumberOfConsumers(int numberOfConsumers) {
        this.numberOfConsumers = numberOfConsumers;
    }

    /**
     * Storage-implementation dependent.
     */
    public abstract void commit(Collection<IHistogramData> data)
                    throws RetryException;

    private void processDroppedData(IHistogramData data) {
        dropped.incrementAndGet();
        logger.info("encountered max queue limit; "
                                    + "{} entries have been dropped",
                    dropped.get());
        logger.debug("queue limit prevented storage of {}", data);
    }

    private void processInterrupted(IHistogramData data) {
        dropped.incrementAndGet();
        logger.warn("queueing of data was interrupted; "
                                    + "{} entries have been dropped",
                    dropped.get());
        logger.debug("failed to store {}", data);
    }
}
