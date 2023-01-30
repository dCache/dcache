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
package org.dcache.services.bulk.store.jdbc.rtarget;

import diskCacheV111.util.PnfsId;
import java.sql.Timestamp;
import org.dcache.db.JdbcCriterion;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.util.BulkRequestTarget.PID;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.services.bulk.util.BulkTargetFilter;

/**
 * Implementation of criterion class for querying the request target table.
 */
public final class JdbcRequestTargetCriterion extends JdbcCriterion {

    public JdbcRequestTargetCriterion() {
        sorter = "request_target.last_updated";
    }

    public JdbcRequestTargetCriterion join() {
        addJoin("bulk_request.id = rid");
        return this;
    }

    public JdbcRequestTargetCriterion id(Long id) {
        addClause("request_target.id = ?", id);
        return this;
    }

    public JdbcRequestTargetCriterion notRootRequest() {
        addClause("pid != ?", PID.ROOT.ordinal());
        return this;
    }

    public JdbcRequestTargetCriterion offset(Long id) {
        addClause("request_target.id >= ?", id);
        return this;
    }

    public JdbcRequestTargetCriterion pids(Integer... pid) {
        addOrClause("pid = ?", o->o, pid);
        return this;
    }

    public JdbcRequestTargetCriterion rid(Long rid) {
        addClause("rid = ?", rid);
        return this;
    }

    public JdbcRequestTargetCriterion ruids(String... ruid) {
        addOrClause("bulk_request.uid = ?", ruid);
        if (ruid != null) {
            return join();
        }
        return this;
    }

    public JdbcRequestTargetCriterion createdBefore(long createdAt) {
        addClause("request_target.created_at <= ?", new Timestamp(createdAt));
        return this;
    }

    public JdbcRequestTargetCriterion createdAfter(long createdAt) {
        addClause("request_target.created_at >= ?", new Timestamp(createdAt));
        return this;
    }

    public JdbcRequestTargetCriterion updatedBefore(long lastUpdated) {
        addClause("request_target.last_updated <= ?", new Timestamp(lastUpdated));
        return this;
    }

    public JdbcRequestTargetCriterion updatedAfter(long lastUpdated) {
        addClause("request_target.last_updated >= ?", new Timestamp(lastUpdated));
        return this;
    }

    public JdbcRequestTargetCriterion state(State... state) {
        addOrClause("state = ?", (Object[]) state);
        return this;
    }

    public JdbcRequestTargetCriterion pnfsid(PnfsId id) {
        if (id != null) {
            addClause("pnfsid = ?", id.toString());
        }
        return this;
    }

    public JdbcRequestTargetCriterion pnfsids(String ... pnfsids) {
        addOrClause("pnfsid = ?", pnfsids);
        return this;
    }

    public JdbcRequestTargetCriterion activity(String ... activity) {
        addOrClause("bulk_request.activity = ?", activity);
        return join();
    }

    public JdbcRequestTargetCriterion type(FileType type) {
        addClause("type = ?", type.name());
        return this;
    }

    public JdbcRequestTargetCriterion type(String ... types) {
        addOrClause("type = ?", types);
        return this;
    }

    public JdbcRequestTargetCriterion errorType(String... errorType) {
        addOrClause("error_type = ?", (Object[]) errorType);
        return this;
    }

    public JdbcRequestTargetCriterion classifier(String classifier) {
        this.classifier = classifier;
        return this;
    }

    public JdbcRequestTargetCriterion sorter(String sorter) {
        this.sorter = sorter;
        return this;
    }

    public JdbcRequestTargetCriterion reverse(Boolean reverse) {
        this.reverse = reverse;
        return this;
    }

    public JdbcRequestTargetCriterion filter(BulkTargetFilter filter) {
        if (filter != null) {
            offset(filter.getOffset());
            pids(filter.getPids());
            ruids(filter.getRids());
            pnfsids(filter.getPnfsIds());
            activity(filter.getActivities());
            type(filter.getTypes());
            state(filter.getStates());
        }
        return this;
    }
}
