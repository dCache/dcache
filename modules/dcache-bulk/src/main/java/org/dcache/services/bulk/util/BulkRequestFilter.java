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

import java.util.Set;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.BulkRequestStatus;

/**
 * Provides filtering for request queries. Converts collections into arrays for easy use by JDBC.
 */
public final class BulkRequestFilter {

    private final Long before;
    private final Long after;
    private final Set<String> owner;
    private final Set<String> urlPrefix;
    private final Set<String> uuid;
    private final Set<String> activity;
    private final Set<BulkRequestStatus> statuses;
    private final Boolean cancelOnFailure;
    private final Boolean clearOnSuccess;
    private final Boolean clearOnFailure;
    private final Boolean delayClear;
    private final Depth expandDirectories;
    private final Boolean prestore;

    private Long id;

    public BulkRequestFilter(Set<BulkRequestStatus> statuses) {
        this(null, null, null, null, null, null,
              statuses, null, null,
              null, null, null, null);
    }

    public BulkRequestFilter(Long before, Long after, Set<String> owner, Set<String> urlPrefix,
          Set<String> uuid, Set<String> activity, Set<BulkRequestStatus> statuses,
          Boolean cancelOnFailure, Boolean clearOnSuccess, Boolean clearOnFailure,
          Boolean delayClear, Depth expandDirectories, Boolean prestore) {
        this.before = before;
        this.after = after;
        this.activity = activity;
        this.statuses = statuses;
        this.cancelOnFailure = cancelOnFailure;
        this.clearOnSuccess = clearOnSuccess;
        this.clearOnFailure = clearOnFailure;
        this.delayClear = delayClear;
        this.expandDirectories = expandDirectories;
        this.owner = owner;
        this.urlPrefix = urlPrefix;
        this.uuid = uuid;
        this.prestore = prestore;
    }

    public Long getAfter() {
        return after;
    }

    public String[] getActivity() {
        return activity == null ? null : activity.toArray(String[]::new);
    }

    public Long getBefore() {
        return before;
    }

    public Boolean getCancelOnFailure() {
        return cancelOnFailure;
    }

    public Boolean getClearOnSuccess() {
        return clearOnSuccess;
    }

    public Boolean getClearOnFailure() {
        return clearOnFailure;
    }

    public Boolean getDelayClear() {
        return delayClear;
    }

    public Boolean getPrestore() { return prestore; }

    public Depth getExpandDirectories() {
        return expandDirectories;
    }

    public String[] getUuids() {
        return id == null ? null : uuid.toArray(String[]::new);
    }

    public String[] getOwner() {
        return owner == null ? null : owner.toArray(String[]::new);
    }

    public Long getId() {
        return id;
    }

    public BulkRequestStatus[] getStatuses() {
        return statuses == null ? null : statuses.toArray(BulkRequestStatus[]::new);
    }

    public String[] getUrlPrefix() {
        return urlPrefix == null ? null : urlPrefix.toArray(String[]::new);
    }

    public void setId(Long id) {
        this.id = id;
    }
}
