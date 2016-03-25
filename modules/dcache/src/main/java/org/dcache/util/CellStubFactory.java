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
package org.dcache.util;

import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;
import org.dcache.cells.CellStub;

/**
 * <p>Convenience factory for constructing CellStubs for
 *      messaging with pools.  Also carries stub for the Pin Manager</p>
 *
 * <p>Class is not final so it can be stubbed for unit testing.</p>
 */
public class CellStubFactory implements CellMessageSender {
    private CellEndpoint cellEndpoint;

    private CellStub pinManager;
    private Long     poolTimeout;
    private TimeUnit poolTimeoutUnit;

    public CellStub getPinManager() {
        return pinManager;
    }

    public CellStub getPoolStub(String destination) {
        CellStub stub = new CellStub();
        stub.setCellEndpoint(cellEndpoint);
        stub.setDestination(destination);
        stub.setRetryOnNoRouteToCell(true);
        if (poolTimeout != null) {
            stub.setTimeout(poolTimeout);
        }
        if (poolTimeoutUnit != null) {
            stub.setTimeoutUnit(poolTimeoutUnit);
        }
        return stub;
    }

    @Override
    public void setCellEndpoint(CellEndpoint cellEndpoint) {
        this.cellEndpoint = cellEndpoint;
    }

    public void setPinManager(CellStub pinManager) {
        this.pinManager = pinManager;
    }

    public void setPoolTimeout(Long poolTimeout) {
        this.poolTimeout = poolTimeout;
    }

    public void setPoolTimeoutUnit(TimeUnit poolTimeoutUnit) {
        this.poolTimeoutUnit = poolTimeoutUnit;
    }
}
