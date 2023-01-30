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
package org.dcache.services.bulk.util;

import diskCacheV111.util.FsPath;
import org.dcache.services.bulk.util.BulkRequestTarget.PID;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.vehicles.FileAttributes;

/**
 *  Fluent builder for creating request targets.
 */
public final class BulkRequestTargetBuilder {

    private final BulkRequestTarget target;

    public static BulkRequestTargetBuilder builder() {
        return new BulkRequestTargetBuilder();
    }

    public BulkRequestTargetBuilder id(Long id) {
        target.setId(id);
        return this;
    }

    public BulkRequestTargetBuilder pid(PID pid) {
        target.setPid(pid);
        return this;
    }

    public BulkRequestTargetBuilder rid(Long rid) {
        target.setRid(rid);
        return this;
    }

    public BulkRequestTargetBuilder ruid(String ruid) {
        target.setRuid(ruid);
        return this;
    }

    public BulkRequestTargetBuilder path(FsPath path) {
        target.setPath(path);
        return this;
    }

    public BulkRequestTargetBuilder attributes(FileAttributes attributes) {
        target.setAttributes(attributes);
        return this;
    }

    public BulkRequestTargetBuilder activity(String activity) {
        target.setActivity(activity);
        return this;
    }

    public BulkRequestTargetBuilder state(State state) {
        target.setState(state);
        return this;
    }

    public BulkRequestTargetBuilder createdAt(long createdAt) {
        target.setCreatedAt(createdAt);
        return this;
    }

    public BulkRequestTargetBuilder startedAt(Long startedAt) {
        target.setStartedAt(startedAt);
        return this;
    }

    public BulkRequestTargetBuilder lastUpdated(long lastUpdated) {
        target.setLastUpdated(lastUpdated);
        return this;
    }

    public BulkRequestTargetBuilder retried(int retried) {
        target.setRetried(retried);
        return this;
    }

    public BulkRequestTargetBuilder error(Object error) {
        target.setErrorObject(error);
        return this;
    }

    public BulkRequestTarget build() {
        return target;
    }

    private BulkRequestTargetBuilder() {
        target = new BulkRequestTarget();
    }
}
