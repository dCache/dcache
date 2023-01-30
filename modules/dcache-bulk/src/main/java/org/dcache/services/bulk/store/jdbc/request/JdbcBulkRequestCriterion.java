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
package org.dcache.services.bulk.store.jdbc.request;

import java.sql.Timestamp;
import java.util.Set;
import org.dcache.db.JdbcCriterion;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.BulkRequestStatus;
import org.dcache.services.bulk.util.BulkRequestFilter;

/**
 * Implementation of criterion class for querying the bulk request table.
 */
public final class JdbcBulkRequestCriterion extends JdbcCriterion {

    public JdbcBulkRequestCriterion() {
        sorter = "arrived_at";
    }

    public JdbcBulkRequestCriterion joinOnTarget() {
        addJoin("bulk_request.id = request_target.rid");
        return this;
    }

    public JdbcBulkRequestCriterion joinOnPermissions() {
        addJoin("bulk_request.id = request_permissions.id");
        return this;
    }

    public JdbcBulkRequestCriterion id(Long id) {
        if (id != null) {
            addClause("bulk_request.id >= ?", id);
        }
        return this;
    }

    public JdbcBulkRequestCriterion pnfsId(String pnfsid) {
        if (pnfsid != null) {
            addClause("request_target.pnfsid = ?", pnfsid);
            joinOnTarget();
        }
        return this;
    }

    public JdbcBulkRequestCriterion permId(String uid) {
        if (uid != null) {
            addClause("bulk_request.uid = ?", uid);
            joinOnPermissions();
        }
        return this;
    }

    public JdbcBulkRequestCriterion uids(String... uids) {
        addOrClause("uid = ?", (Object[]) uids);
        return this;
    }

    public JdbcBulkRequestCriterion user(String... user) {
        addOrClause("owner = ?", (Object[]) user);
        return this;
    }

    public JdbcBulkRequestCriterion activity(String... activity) {
        addOrClause("bulk_request.activity = ?", (Object[]) activity);
        return this;
    }

    public JdbcBulkRequestCriterion activity(Set<String> activity) {
        if (activity != null) {
            activity(activity.toArray(String[]::new));
        }
        return this;
    }

    public JdbcBulkRequestCriterion arrivedBefore(Long arrivedAt) {
        if (arrivedAt != null) {
            addClause("arrived_at <= ?", new Timestamp(arrivedAt));
        }
        return this;
    }

    public JdbcBulkRequestCriterion arrivedAfter(Long arrivedAt) {
        if (arrivedAt != null) {
            addClause("arrived_at >= ?", new Timestamp(arrivedAt));
        }
        return this;
    }

    public JdbcBulkRequestCriterion startedBefore(Long startedAt) {
        if (startedAt != null) {
            addClause("started_at <= ?", new Timestamp(startedAt));
        }
        return this;
    }

    public JdbcBulkRequestCriterion startedAfter(Long startedAt) {
        if (startedAt != null) {
            addClause("started_at >= ?", new Timestamp(startedAt));
        }
        return this;
    }

    public JdbcBulkRequestCriterion modifiedBefore(Long lastModified) {
        if (lastModified != null) {
            addClause("last_modified <= ?", new Timestamp(lastModified));
        }
        return this;
    }

    public JdbcBulkRequestCriterion modifiedAfter(Long lastModified) {
        if (lastModified != null) {
            addClause("last_modified >= ?", new Timestamp(lastModified));
        }
        return this;
    }

    public JdbcBulkRequestCriterion cancelOnFailure(Boolean cancelOnFailure) {
        if (cancelOnFailure != null) {
            addClause("cancel_on_failure = ?", cancelOnFailure);
        }
        return this;
    }

    public JdbcBulkRequestCriterion clearOnFailure(Boolean clearOnFailure) {
        if (clearOnFailure != null) {
            addClause("clear_on_failure = ?", clearOnFailure);
        }
        return this;
    }

    public JdbcBulkRequestCriterion clearOnSuccess(Boolean clearOnSuccess) {
        if (clearOnSuccess != null) {
            addClause("clear_on_success = ?", clearOnSuccess);
        }
        return this;
    }

    public JdbcBulkRequestCriterion prestore(Boolean prestore) {
        if (prestore != null) {
            addClause("prestore = ?", prestore);
        }
        return this;
    }

    public JdbcBulkRequestCriterion delayClear(Boolean delayClear) {
        if (delayClear != null) {
            addClause("delay_clear = ?", delayClear);
        }
        return this;
    }

    public JdbcBulkRequestCriterion expandDirectories(Depth depth) {
        if (depth != null) {
            addClause("expand_directories = ?", depth.name());
        }
        return this;
    }

    public JdbcBulkRequestCriterion status(BulkRequestStatus... status) {
        addOrClause("status = ?", (Object[]) status);
        return this;
    }

    public JdbcBulkRequestCriterion status(Set<BulkRequestStatus> status) {
        if (status != null) {
            status(status.toArray(BulkRequestStatus[]::new));
        }
        return this;
    }

    public JdbcBulkRequestCriterion classifier(String classifier) {
        this.classifier = classifier;
        return this;
    }

    public JdbcBulkRequestCriterion sorter(String sorter) {
        this.sorter = sorter;
        return this;
    }

    public JdbcBulkRequestCriterion reverse(Boolean reverse) {
        this.reverse = reverse;
        return this;
    }

    public JdbcBulkRequestCriterion filter(BulkRequestFilter filter) {
        if (filter != null) {
            id(filter.getId());
            arrivedBefore(filter.getBefore());
            arrivedAfter(filter.getAfter());
            cancelOnFailure(filter.getCancelOnFailure());
            clearOnFailure(filter.getClearOnFailure());
            clearOnSuccess(filter.getClearOnSuccess());
            prestore(filter.getPrestore());
            delayClear(filter.getDelayClear());
            expandDirectories(filter.getExpandDirectories());
            activity(filter.getActivity());
            status(filter.getStatuses());
            uids(filter.getUuids());
            user(filter.getOwner());
        }
        return this;
    }
}
