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
package org.dcache.qos;

import static com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.pool.migration.PoolMigrationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles synchronous behavior for QoS.  When the caller wishes to wait for completion of either
 * staging or migration, it must add an implementation of this class to the QoSTransitionEngine.
 */
public abstract class QoSReplyHandler {

    private static final Logger LOGGER
          = LoggerFactory.getLogger(QoSReplyHandler.class);

    private final Executor executor;

    protected PoolMigrationMessage migrationReply;
    protected PinManagerPinMessage pinReply;

    private ListenableFuture<PoolMigrationMessage> migrationFuture;
    private ListenableFuture<PinManagerPinMessage> pinFuture;

    protected QoSReplyHandler(Executor executor) {
        this.executor = executor;
    }

    public synchronized void cancel() {
        if (migrationFuture != null && migrationReply == null) {
            migrationFuture.cancel(true);
            LOGGER.debug("Cancelled migrationFuture.");
            migrationFuture = null;
        }

        if (pinFuture != null && pinReply == null) {
            pinFuture.cancel(true);
            LOGGER.debug("Cancelled pinFuture.");
            pinFuture = null;
        }
    }

    public String toString() {
        return "(migration " + migrationFuture + ", "
              + migrationReply + ")(pin "
              + pinFuture + ", " + pinReply + ")";
    }

    public synchronized boolean done() {
        LOGGER.trace("done called: {}.", this);
        return (migrationFuture == null || migrationReply != null) &&
              (pinFuture == null || pinReply != null);
    }

    public void listen() {
        if (migrationFuture != null) {
            migrationFuture.addListener(() -> handleMigrationReply(), executor);
        }

        if (pinFuture != null) {
            pinFuture.addListener(() -> handlePinReply(), executor);
        }

        LOGGER.trace("listen called: {}.", this);
    }

    public synchronized void setMigrationFuture(ListenableFuture<PoolMigrationMessage> future) {
        this.migrationFuture = future;
    }

    public synchronized void setPinFuture(ListenableFuture<PinManagerPinMessage> future) {
        this.pinFuture = future;
    }

    protected abstract void migrationFailure(Object error);

    protected abstract void migrationSuccess();

    protected abstract void pinFailure(Object error);

    protected abstract void pinSuccess();

    private synchronized void handleMigrationReply() {
        if (migrationFuture == null) {
            LOGGER.debug("No migration future set, no reply expected.");
            return;
        }

        Object error = null;
        try {
            migrationReply = getUninterruptibly(migrationFuture);
            if (migrationReply.getReturnCode() != 0) {
                error = migrationReply.getErrorObject();
            }
        } catch (CancellationException e) {
            /*
             *  Cancelled state should be set by caller.
             */
        } catch (ExecutionException e) {
            error = e.getCause();
        }

        if (error != null) {
            migrationFailure(error);
        } else {
            migrationSuccess();
        }
    }

    private synchronized void handlePinReply() {
        if (pinFuture == null) {
            LOGGER.debug("No pin future set, no reply expected.");
            return;
        }

        Object error = null;
        try {
            pinReply = getUninterruptibly(pinFuture);
            if (pinReply.getReturnCode() != 0) {
                error = pinReply.getErrorObject();
            }
        } catch (ExecutionException e) {
            error = e.getCause();
        }

        if (error != null) {
            pinFailure(error);
        } else {
            pinSuccess();
        }
    }
}
