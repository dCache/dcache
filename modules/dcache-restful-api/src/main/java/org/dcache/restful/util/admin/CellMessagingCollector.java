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
package org.dcache.restful.util.admin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;
import org.dcache.cells.CellStub;

/**
 * <p>The primary function of the services in support of
 *    the RESTful admin resources is to gather information
 *    from other dCache cells via messaging, and to cache it.</p>
 *
 * <p>This class provides the common API for the collection of data.</p>
 */
public abstract class CellMessagingCollector<D> implements CellMessageSender {
    protected static final Logger LOGGER
                    = LoggerFactory.getLogger(CellMessagingCollector.class);

    /**
     * <p>For querying various endpoints during information collection.</p>
     */
    protected final CellStub stub = new CellStub();

    /**
     * <p>Should be overridden to do any special internal initialization.</p>
     *
     * @param timeout cell timeout value.
     * @param timeUnit cell timeout unit.
     */
    public void initialize(Long timeout, TimeUnit timeUnit) {
        reset(timeout, timeUnit);
    }

    /**
     * <p>Provides for dynamic changes to timeout period, e.g., from
     *      an admin command.</p>
     *
     * <p>Should be called at least at initialization.</p>
     *
     * @param timeout cell timeout value.
     * @param timeUnit cell timeout unit.
     */
    public void reset(Long timeout, TimeUnit timeUnit) {
        stub.setTimeout(timeout);
        stub.setTimeoutUnit(timeUnit);
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        stub.setCellEndpoint(endpoint);
    }

    /**
     * <p>Should be overridden to do any special internal cleanup.</p>
     */
    public void shutdown() {
        //NOP
    }

    /**
     * <p>Should do the actual collection.</p>
     */
    public abstract D collectData() throws InterruptedException;
}
