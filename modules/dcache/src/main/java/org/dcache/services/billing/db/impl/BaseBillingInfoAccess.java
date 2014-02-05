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

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.exceptions.RetryException;
import org.dcache.services.billing.histograms.data.IHistogramData;

/**
 * Framework for database access; composes delegate for handling inserts.
 *
 * @author arossi
 */
public abstract class BaseBillingInfoAccess implements IBillingInfoAccess {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private String delegateType;
    private QueueDelegate delegate;
    private int maxQueueSize;
    private int maxBatchSize;
    private boolean dropMessagesAtLimit;

    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }

    public abstract void commit(Collection<IHistogramData> data)
                    throws RetryException;

    public long getCommittedMessages() {
        if (delegate == null) {
            return 0;
        }
        return delegate.getCommitted();
    }

    public long getDroppedMessages() {
        if (delegate == null) {
            return 0;
        }
        return delegate.getDropped();
    }

    public long getInsertQueueSize() {
        if (delegate == null) {
            return 0;
        }
        return delegate.getQueueSize();
    }

    public void initialize() {
        logger.debug("access type: {}", this.getClass().getName());

        /*
         * can be null (configuration for read-only access; see #put())
         */
        if (delegateType != null) {
            Class<?> clzz;
            try {
                clzz = Class.forName(delegateType);
            } catch (ClassNotFoundException t) {
                throw new RuntimeException(t);
            }
            try {
                delegate = (QueueDelegate) clzz.newInstance();
            } catch (InstantiationException | IllegalAccessException t) {
                throw new RuntimeException(t);
            }
            delegate.setDropMessagesAtLimit(dropMessagesAtLimit);
            delegate.setMaxQueueSize(maxQueueSize);
            delegate.setMaxBatchSize(maxBatchSize);
            delegate.setCallback(this);
            delegate.initialize();
            logger.debug("delegate type: {}", clzz);
        }
    }

    public void put(IHistogramData data) {
        if (delegate == null) {
            logger.warn("attempting to insert data but database access has not"
                            + " been initialized to handle inserts; please set the "
                            + "billing.db.inserts.queue-delegate.type property");
            return;
        }

        delegate.handlePut(data);
    }

    public void setDelegateType(String delegateType) {
        this.delegateType = Strings.emptyToNull(delegateType);
    }

    public void setDropMessagesAtLimit(boolean dropMessagesAtLimit) {
        this.dropMessagesAtLimit = dropMessagesAtLimit;
    }

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }
}
