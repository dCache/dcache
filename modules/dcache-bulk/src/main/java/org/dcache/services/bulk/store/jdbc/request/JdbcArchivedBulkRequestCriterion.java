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

import static org.dcache.services.bulk.util.BulkServiceStatistics.getTimestamp;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Set;
import org.dcache.db.JdbcCriterion;
import org.dcache.services.bulk.BulkArchivedSummaryFilter;

/**
 * Implementation of criterion class for querying the request archive table.
 */
public final class JdbcArchivedBulkRequestCriterion extends JdbcCriterion {

    public JdbcArchivedBulkRequestCriterion() {
        sorter = "last_modified";
    }

    public JdbcArchivedBulkRequestCriterion uids(String... uids) {
        addOrClause("uid = ?", (Object[]) uids);
        return this;
    }

    public JdbcArchivedBulkRequestCriterion owner(String... owner) {
        addOrClause("owner = ?", (Object[]) owner);
        return this;
    }

    public JdbcArchivedBulkRequestCriterion activity(String... activity) {
        addOrClause("activity = ?", (Object[]) activity);
        return this;
    }

    public JdbcArchivedBulkRequestCriterion modifiedBefore(Long lastModified) {
        if (lastModified != null) {
            addClause("last_modified <= ?", new Timestamp(lastModified));
        }
        return this;
    }

    public JdbcArchivedBulkRequestCriterion modifiedAfter(Long lastModified) {
        if (lastModified != null) {
            addClause("last_modified >= ?", new Timestamp(lastModified));
        }
        return this;
    }

    public JdbcArchivedBulkRequestCriterion status(String ... status) {
        addOrClause("status = ?", (Object[]) status);
        return this;
    }

    public JdbcArchivedBulkRequestCriterion fromFilter(BulkArchivedSummaryFilter filter)
          throws ParseException {
        activity(toArray(filter.getActvity()));
        owner(toArray(filter.getOwner()));
        status(toArray(filter.getStatus()));
        modifiedBefore(getTimestamp(filter.getBefore()));
        modifiedAfter(getTimestamp(filter.getAfter()));
        return this;
    }

    public JdbcArchivedBulkRequestCriterion classifier(String classifier) {
        this.classifier = classifier;
        return this;
    }

    public JdbcArchivedBulkRequestCriterion sorter(String sorter) {
        this.sorter = sorter;
        return this;
    }

    public JdbcArchivedBulkRequestCriterion reverse(Boolean reverse) {
        this.reverse = reverse;
        return this;
    }

    private static String[] toArray(Set<String> set) {
        if (set == null) {
            return null;
        }

        return set.toArray(String[]::new);
    }
}
