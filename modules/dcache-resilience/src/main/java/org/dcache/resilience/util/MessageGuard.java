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
package org.dcache.resilience.util;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.UUID;

import dmg.cells.nucleus.CDC;

/**
 * <p>Used to check whether the incoming message carries the replication
 *      handler session id. This is necessary to distinguish messages
 *      which require handling from those which do not.</p>
 *
 * <p>Also stores messages which arrive during the DISABLED state, and
 *      provides them on a one-time-basis to the caller,
 *      when re-enabled.</p>
 *
 * <p>Class is not marked final for stubbing/mocking purposes.</p>
 */
public class MessageGuard {
    private static final String RESILIENCE_KEY = "RESILIENCE-";

    @VisibleForTesting
    static final String RESILIENCE_ID  = RESILIENCE_KEY + UUID.randomUUID();

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageGuard.class);

    static void setResilienceSession() {
        CDC.setSession(RESILIENCE_ID);
    }

    public enum Status {
        DISABLED,        // replication handler is not accepting messages
        REPLICA,         // message contains the replication session id
        EXTERNAL         // message does not contain replication session id
    }

    private BackloggedMessageHandler backlogHandler;
    private boolean                  enabled = false;
    private boolean                  dropMessages = false;

    /**
     * @param message       informative statement for logging purposes.
     * @param messageObject received by the handler.
     * @return status of the message (EXCLUDED, REPLICA, EXTERNAL).
     */
    public Status getStatus(String message, Object messageObject) {
        LOGGER.trace("**** acceptMessage **** {}: {} -- {}.", message,
                     messageObject, enabled);

        String session = CDC.getSession();
        LOGGER.trace("{} – session {}", message, session);

        synchronized (backlogHandler) {
            if (!enabled) {
                if (!dropMessages && !isResilientSession(session)
                                  && messageObject instanceof Serializable) {
                    backlogHandler.saveToBacklog((Serializable) messageObject);
                }
                LOGGER.trace("{}: {}.", message, Status.DISABLED);
                return Status.DISABLED;
            }
        }

        if (isResilientSession(session)) {
            LOGGER.trace("{} originated within the replication system ({}).",
                            message, RESILIENCE_ID);
            return Status.REPLICA;
        }

        LOGGER.trace("{}: {}.", message, Status.EXTERNAL);
        return Status.EXTERNAL;
    }

    public boolean isEnabled() {
        synchronized (backlogHandler) {
            return enabled;
        }
    }

    public void setBacklogHandler(BackloggedMessageHandler backlogHandler) {
        this.backlogHandler = backlogHandler;
    }

    public void disable(boolean drop) {
        synchronized (backlogHandler) {
            enabled = false;
            dropMessages = drop;
            if (!drop) {
                backlogHandler.initialize();
            } else {
                backlogHandler.shutdown();
            }
        }
    }

    public void enable() {
        synchronized (backlogHandler) {
            enabled = true;
            backlogHandler.handleBacklog();
        }
    }

    private boolean isResilientSession(String session) {
        return session != null && session.startsWith(RESILIENCE_KEY);
    }
}
