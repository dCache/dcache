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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.BulkRequestInfo;
import org.dcache.services.bulk.BulkRequestStatus;
import org.dcache.services.bulk.BulkRequestStatusInfo;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.store.jdbc.JdbcBulkDaoUtils;
import org.dcache.services.bulk.store.jdbc.rtarget.JdbcRequestTargetDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.KeyHolder;

/**
 * CRUD for the bulk request table.
 */
public final class JdbcBulkRequestDao extends JdbcDaoSupport {

    public static final String TABLE_NAME = "bulk_request";

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcBulkRequestDao.class);

    private static final String SELECT = "SELECT bulk_request.*";

    private static final String JOINED_TABLE_NAMES =
          TABLE_NAME + ", " + JdbcRequestTargetDao.TABLE_NAME;

    /**
     * Update queries which permit the avoidance of fetch-and-set semantics requiring in-memory
     * synchronization.
     */
    private static final String NO_UNPROCESSED_TARGETS = "NOT EXISTS "
          + "(SELECT * FROM request_target t WHERE t.rid = r.id AND "
          + "(t.state = 'CREATED' OR t.state = 'READY' OR t.state='RUNNING'))";

    private static final String UPDATE_COMPLETED_IF_DONE =
          "UPDATE bulk_request r SET status='COMPLETED', last_modified = ? WHERE r.id = ? AND "
                + NO_UNPROCESSED_TARGETS;

    private static final String UPDATE_CANCELLED_IF_DONE =
          "UPDATE bulk_request r SET status='CANCELLED', last_modified = ? WHERE r.id = ? "
                + "AND r.status='CANCELLING' AND " + NO_UNPROCESSED_TARGETS;

    private JdbcBulkDaoUtils utils;

    public int count(JdbcBulkRequestCriterion criterion) {
        return utils.count(criterion, TABLE_NAME, this);
    }

    /*
     * Should delete the jobs by cascading on the request id.
     */
    public int delete(JdbcBulkRequestCriterion criterion) {
        return utils.delete(criterion, TABLE_NAME, this);
    }

    public List<BulkRequest> get(JdbcBulkRequestCriterion criterion, int limit) {
        if (criterion.isJoined()) {
            return utils.get(SELECT, criterion, limit, JOINED_TABLE_NAMES, this, this::toRequest);
        }
        return utils.get(criterion, limit, TABLE_NAME, this, this::toRequest);
    }

    public Optional<KeyHolder> insert(JdbcBulkRequestUpdate update) {
        return utils.insert(update, TABLE_NAME, this);
    }

    public JdbcBulkRequestUpdate set() {
        return new JdbcBulkRequestUpdate(utils);
    }

    @Required
    public void setUtils(JdbcBulkDaoUtils utils) {
        this.utils = utils;
    }

    /**
     * Based on the ResultSet returned by the query, construct a BulkRequest object for a given
     * request.
     *
     * @param rs  from the query.
     * @param row unused, but needs to be there to satisfy the template function signature.
     * @return request wrapper object.
     * @throws SQLException if access to the ResultSet fails or there is a deserialization error.
     */
    public BulkRequest toRequest(ResultSet rs, int row) throws SQLException {
        BulkRequest request = new BulkRequest();
        request.setSeqNo(rs.getLong("seq_no"));
        String id = rs.getString("id");
        request.setId(id);
        request.setActivity(rs.getString("activity"));
        request.setExpandDirectories(Depth.valueOf(rs.getString("expand_directories")));
        request.setUrlPrefix(rs.getString("url_prefix"));
        request.setTargetPrefix(rs.getString("target_prefix"));
        request.setClearOnSuccess(rs.getBoolean("clear_on_success"));
        request.setClearOnFailure(rs.getBoolean("clear_on_failure"));
        request.setCancelOnFailure(rs.getBoolean("cancel_on_failure"));
        request.setPrestore(rs.getBoolean("prestore"));
        String args = rs.getString("arguments");
        if (Strings.emptyToNull(args) != null) {
            request.setArguments(Splitter.on(",").withKeyValueSeparator(":").split(args));
        }
        BulkRequestStatusInfo statusInfo = new BulkRequestStatusInfo();
        statusInfo.setUser(rs.getString("owner"));
        statusInfo.setCreatedAt(rs.getTimestamp("arrived_at").getTime());
        Timestamp startedAt = rs.getTimestamp("started_at");
        if (startedAt != null) {
            statusInfo.setStartedAt(startedAt.getTime());
        }
        statusInfo.setLastModified(rs.getTimestamp("last_modified").getTime());
        statusInfo.setStatus(BulkRequestStatus.valueOf(rs.getString("status")));
        if (statusInfo.getStatus() == BulkRequestStatus.COMPLETED
              || statusInfo.getStatus() == BulkRequestStatus.CANCELLED) {
            statusInfo.setCompletedAt(statusInfo.getLastModified());
        }
        request.setStatusInfo(statusInfo);
        LOGGER.debug("toRequest, returning request object {}.", request);
        return request;
    }

    /**
     * Based on the ResultSet returned by the query, construct a BulkRequestInfo for a given
     * request.
     *
     * @param rs  from the query.
     * @param row unused, but needs to be there to satisfy the template function signature.
     * @return info object.
     * @throws SQLException if access to the ResultSet fails or there is a deserialization error.
     */
    public BulkRequestInfo toStatus(ResultSet rs, int row) throws SQLException {
        BulkRequestInfo status = new BulkRequestInfo();
        status.setArrivedAt(rs.getTimestamp("arrived_at").getTime());
        status.setLastModified(rs.getTimestamp("last_modified").getTime());
        Timestamp startedAt = rs.getTimestamp("started_at");
        if (startedAt != null) {
            status.setStartedAt(startedAt.getTime());
        }
        status.setStatus(BulkRequestStatus.valueOf(rs.getString("status")));
        return status;
    }

    public int update(JdbcBulkRequestCriterion criterion, JdbcBulkRequestUpdate update) {
        return utils.update(criterion, update, TABLE_NAME, this);
    }

    /**
     * This method slightly violates the programming model of the dao, but is here in order to allow
     * us not to synchronize when checking and setting the final state of the request.
     *
     * @param status either CANCELLED or COMPLETED.
     * @param id     of the request.
     * @return whether the update succeeded.  False means there are still incomplete targets.
     */
    public int updateTo(BulkRequestStatus status, String id) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        switch (status) {
            case COMPLETED:
                return getJdbcTemplate().update(UPDATE_COMPLETED_IF_DONE,
                      new Object[]{now, id});
            case CANCELLED:
                return getJdbcTemplate().update(UPDATE_CANCELLED_IF_DONE,
                      new Object[]{now, id});
            default:
                return 0;
        }
    }

    public JdbcBulkRequestUpdate updateFrom(BulkRequest request, String user)
          throws BulkStorageException {
        return set().activity(request.getActivity()).arguments(request.getArguments())
              .cancelOnFailure(request.isCancelOnFailure()).id(request.getId())
              .clearOnSuccess(request.isClearOnSuccess()).clearOnFailure(request.isClearOnFailure())
              .prestore(request.isPrestore())
              .depth(request.getExpandDirectories())
              .targetPrefix(request.getTargetPrefix()).urlPrefix(request.getUrlPrefix()).user(user)
              .status(BulkRequestStatus.QUEUED).arrivedAt(System.currentTimeMillis());
    }

    public JdbcBulkRequestCriterion where() {
        return new JdbcBulkRequestCriterion();
    }
}
