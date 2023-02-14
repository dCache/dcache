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

import com.google.common.base.Strings;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.BulkRequestInfo;
import org.dcache.services.bulk.BulkRequestStatus;
import org.dcache.services.bulk.BulkRequestStatusInfo;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.store.jdbc.JdbcBulkDaoUtils;
import org.dcache.services.bulk.store.jdbc.rtarget.JdbcRequestTargetDao;
import org.json.JSONObject;
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

    private static final String ARGUMENTS_TABLE_NAME = "request_arguments";

    private static final String INSERT_ARGS =
          "INSERT INTO " + ARGUMENTS_TABLE_NAME + " VALUES (?, ?)";

    private static final String SELECT = "SELECT bulk_request.*";

    private static final String FULL_SELECT =
          "SELECT " + TABLE_NAME + ".*, " + ARGUMENTS_TABLE_NAME + ".arguments as arguments";

    private static final String JOINED_WITH_TARGET_TABLE_NAMES =
          TABLE_NAME + ", " + JdbcRequestTargetDao.TABLE_NAME;

    private static final String JOINED_WITH_ARGUMENTS_TABLE_NAMES =
          TABLE_NAME + " LEFT OUTER JOIN " + ARGUMENTS_TABLE_NAME
                + " ON bulk_request.id = request_arguments.rid";

    /**
     * Update queries which permit the avoidance of fetch-and-set semantics requiring in-memory
     * synchronization.
     */
    private static final String NO_UNPROCESSED_TARGETS = "NOT EXISTS "
          + "(SELECT * FROM request_target t WHERE t.rid = r.id AND "
          + "(t.state = 'CREATED' OR t.state = 'READY' OR t.state='RUNNING'))";

    private static final String UPDATE_COMPLETED_IF_DONE =
          "UPDATE bulk_request r SET status='COMPLETED', last_modified = ? WHERE r.uid = ? AND "
                + NO_UNPROCESSED_TARGETS;

    private static final String UPDATE_CANCELLED_IF_DONE =
          "UPDATE bulk_request r SET status='CANCELLED', last_modified = ? WHERE r.uid = ? "
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

    public List<BulkRequest> get(JdbcBulkRequestCriterion criterion, int limit,
          boolean includeArgs) {
        if (criterion.isJoined()) {
            return utils.get(SELECT, criterion, limit, JOINED_WITH_TARGET_TABLE_NAMES, this,
                  this::toRequest);
        }

        if (includeArgs) {
            return utils.get(FULL_SELECT, criterion, limit, JOINED_WITH_ARGUMENTS_TABLE_NAMES, this,
                  this::toFullRequest);
        } else {
            return utils.get(criterion, limit, TABLE_NAME, this, this::toRequest);
        }
    }

    public Optional<KeyHolder> insert(JdbcBulkRequestUpdate update) {
        return utils.insert(update, TABLE_NAME, this);
    }

    public void insertArguments(BulkRequest request) {
        Map<String, String> arguments = request.getArguments();
        if (arguments != null && !arguments.isEmpty()) {
            String argumentsText = arguments.entrySet().stream()
                  .map(e -> e.getKey() + ":" + e.getValue())
                  .collect(Collectors.joining(","));
            utils.insert(INSERT_ARGS, List.of(request.getId(), argumentsText), this);
        }
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
     * request.  Does not include arguments.
     *
     * @param rs  from the query.
     * @param row unused, but needs to be there to satisfy the template function signature.
     * @return request wrapper object.
     * @throws SQLException if access to the ResultSet fails or there is a deserialization error.
     */
    public BulkRequest toRequest(ResultSet rs, int row) throws SQLException {
        BulkRequest request = new BulkRequest();
        request.setId(rs.getLong("id"));
        String uid = rs.getString("uid");
        request.setUid(uid);
        request.setActivity(rs.getString("activity"));
        request.setExpandDirectories(Depth.valueOf(rs.getString("expand_directories")));
        request.setUrlPrefix(rs.getString("url_prefix"));
        request.setTargetPrefix(rs.getString("target_prefix"));
        request.setClearOnSuccess(rs.getBoolean("clear_on_success"));
        request.setClearOnFailure(rs.getBoolean("clear_on_failure"));
        request.setCancelOnFailure(rs.getBoolean("cancel_on_failure"));
        request.setPrestore(rs.getBoolean("prestore"));
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
     * Based on the ResultSet returned by the query, construct a BulkRequest object for a given
     * request.  Includes arguments.
     *
     * @param rs  from the query.
     * @param row unused, but needs to be there to satisfy the template function signature.
     * @return request wrapper object.
     * @throws SQLException if access to the ResultSet fails or there is a deserialization error.
     */
    public BulkRequest toFullRequest(ResultSet rs, int row) throws SQLException {
        BulkRequest request = toRequest(rs, row);
        String args = rs.getString("arguments");
        if (Strings.emptyToNull(args) != null) {
            JSONObject argObj = new JSONObject("{" + args + "}");
            Map<String, String> arguments = new HashMap<>();
            for (Iterator<String> keys = argObj.keys(); keys.hasNext(); ) {
                String key = keys.next();
                arguments.put(key, String.valueOf(argObj.get(key)));
            }
            request.setArguments(arguments);
        }
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
     * @param uuid   of the request.
     * @return whether the update succeeded.  False means there are still incomplete targets.
     */
    public int updateTo(BulkRequestStatus status, String uuid) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        switch (status) {
            case COMPLETED:
                return getJdbcTemplate().update(UPDATE_COMPLETED_IF_DONE,
                      new Object[]{now, uuid});
            case CANCELLED:
                return getJdbcTemplate().update(UPDATE_CANCELLED_IF_DONE,
                      new Object[]{now, uuid});
            default:
                return 0;
        }
    }

    public JdbcBulkRequestUpdate updateFrom(BulkRequest request, String user)
          throws BulkStorageException {
        return set().activity(request.getActivity())
              .cancelOnFailure(request.isCancelOnFailure()).uid(request.getUid())
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
