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
package org.dcache.services.bulk;

import java.io.Serializable;

/**
 *  Generic bulk status.  In addition to the required fields,
 *  tracks timestamps.
 */
public class BulkRequestStatus implements Serializable
{
    private static final long serialVersionUID = -7992364565691790254L;

    public enum Status
    {
        QUEUED, STARTED, COMPLETED, CANCELLING, CANCELLED
    }

    private long         firstArrived;
    private long         lastModified;
    private Status       status;
    private int          targets;
    private int          processed;
    private BulkFailures failures;

    public long getFirstArrived()
    {
        return firstArrived;
    }

    public long getLastModified()
    {
        return lastModified;
    }

    public Status getStatus()
    {
        return status;
    }

    public void setStatus(Status status)
    {
        this.status = status;
        lastModified = System.currentTimeMillis();
    }

    public int getTargets()
    {
        return targets;
    }

    public void targetAdded()
    {
        ++targets;
    }

    public void setFirstArrived(long firstArrived)
    {
        this.firstArrived = firstArrived;
    }

    public void setLastModified(long lastModified)
    {
        this.lastModified = lastModified;
    }

    public void setTargets(int targets)
    {
        this.targets = targets;
        lastModified = System.currentTimeMillis();
    }

    public int getProcessed()
    {
        return processed;
    }

    public void setProcessed(int processed)
    {
        this.processed = processed;
        lastModified = System.currentTimeMillis();
    }

    public BulkFailures getFailures()
    {
        return failures;
    }

    public void setFailures(BulkFailures failures)
    {
        this.failures = failures;
        lastModified = System.currentTimeMillis();
    }

    public void targetAborted(String target, Throwable exception)
    {
        if (target != null) {
            addException(target, exception);
        }

        lastModified = System.currentTimeMillis();
    }

    public void targetCompleted(String target, Throwable exception)
    {
        if (target != null) {
            ++processed;
            addException(target, exception);
        }

        lastModified = System.currentTimeMillis();
    }

    private void addException(String target, Throwable exception)
    {
        if (exception != null) {
            if (failures == null) {
                failures = new BulkFailures();
            }
            failures.put(target, exception);
        }
    }
}
