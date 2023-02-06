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

import static org.dcache.services.bulk.util.BulkRequestTarget.State.CREATED;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.FAILED;
import static org.dcache.services.bulk.util.BulkRequestTarget.computeFsPath;
import static org.dcache.util.Strings.truncate;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.store.jdbc.JdbcBulkDaoUtils;
import org.dcache.services.bulk.store.jdbc.request.JdbcBulkRequestDao;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkRequestTarget.PID;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.services.bulk.util.BulkRequestTargetBuilder;
import org.dcache.services.bulk.util.BulkServiceStatistics;
import org.dcache.vehicles.FileAttributes;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.KeyHolder;

/**
 * CRUD for the request target table.
 */
public final class JdbcRequestTargetDao extends JdbcDaoSupport {

    public static final String TABLE_NAME = "request_target";

    static class TargetPlaceholder {
        Long rid;
        String path;
        String state;
    }

    static final String BATCH_INSERT = "INSERT INTO " + TABLE_NAME + " ("
          + "pid, rid, pnfsid, path, type, state, created_at, last_updated) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    static final String SELECT = "SELECT *";

    static final String JOINED_SELECT = "SELECT request_target.*, bulk_request.uid as ruid, bulk_request.activity";

    static final String SECONDARY_TABLE_NAME = JdbcBulkRequestDao.TABLE_NAME;

    static final String JOINED_TABLE_NAMES_FOR_SELECT = SECONDARY_TABLE_NAME + ", " + TABLE_NAME;

    static final ParameterizedPreparedStatementSetter<TargetPlaceholder> SETTER = (ps, target) -> {
        Instant now = Instant.now();
        ps.setInt(1, PID.INITIAL.ordinal());
        ps.setLong(2, target.rid);
        ps.setString(3, "?");
        ps.setString(4, target.path);
        ps.setString(5, "?");
        ps.setString(6, target.state);
        ps.setTimestamp(7, Timestamp.from(now));
        ps.setTimestamp(8, Timestamp.from(now));
    };

    private static String tableNameForSelect(JdbcRequestTargetCriterion criterion) {
        return criterion.isJoined() ? JOINED_TABLE_NAMES_FOR_SELECT : TABLE_NAME;
    }

    private static String getSelect(JdbcRequestTargetCriterion criterion) {
        return criterion.isJoined() ? JOINED_SELECT : SELECT;
    }

    private JdbcBulkDaoUtils utils;
    private BulkServiceStatistics statistics;

    public int count(JdbcRequestTargetCriterion criterion) {
        return utils.count(criterion, tableNameForSelect(criterion), this);
    }

    public Map<String, Long> count(JdbcRequestTargetCriterion criterion, String classifier) {
        return utils.countGrouped(criterion.classifier(classifier), tableNameForSelect(criterion), this);
    }

    public Map<String, Long> countStates() {
        return utils.countGrouped(where().classifier("state"), TABLE_NAME, this);
    }

    public int delete(JdbcRequestTargetCriterion criterion) {
        if (criterion.isJoined()) {
            return utils.delete(criterion, TABLE_NAME, SECONDARY_TABLE_NAME, this);
        }
        return utils.delete(criterion, TABLE_NAME, this);
    }

    public List<BulkRequestTarget> get(JdbcRequestTargetCriterion criterion, int limit) {
        return utils.get(getSelect(criterion), criterion, limit, tableNameForSelect(criterion),
              this, criterion.isJoined() ? this::toFullRequestTarget : this::toRequestTarget);
    }

    public Optional<KeyHolder> insert(JdbcRequestTargetUpdate update) {
        return utils.insert(update, TABLE_NAME, this);
    }

    public void insertInitialTargets(BulkRequest request) {
        List<TargetPlaceholder> targets = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (String target : request.getTarget()) {
            TargetPlaceholder t = new TargetPlaceholder();
            t.rid = request.getId();
            String path = target.trim();
            if (path.isEmpty()) {
                t.path = "invalid (empty) path";
                t.state = FAILED.name();
            } else {
                t.path = truncate(target, 256, true);
                t.state = CREATED.name();
            }
            if (seen.contains(t.path)) {
                continue;
            }
            targets.add(t);
            seen.add(t.path);
        }
        utils.insertBatch(targets, BATCH_INSERT, SETTER, this);
    }

    public JdbcRequestTargetUpdate set() {
        return new JdbcRequestTargetUpdate();
    }

    @Required
    public void setUtils(JdbcBulkDaoUtils utils) {
        this.utils = utils;
    }

    @Required
    public void setStatistics(BulkServiceStatistics statistics) {
        this.statistics = statistics;
    }

    public BulkRequestTarget toFullRequestTarget(ResultSet rs, int row) throws SQLException {
        BulkRequestTarget target = toRequestTarget(rs, row);
        target.setRuid(rs.getString("ruid"));
        target.setActivity(rs.getString("activity"));
        return target;
    }

    public BulkRequestTarget toRequestTarget(ResultSet rs, int row) throws SQLException {
        FileAttributes attributes = new FileAttributes();

        String value = rs.getString("pnfsid");
        if (!value.strip().equals("?")) {
            attributes.setPnfsId(new PnfsId(value));
        }

        value = rs.getString("type");
        if (!value.strip().equals("?")) {
            attributes.setFileType(FileType.valueOf(value));
        }

        if (attributes.isUndefined(FileAttribute.TYPE)) {
            attributes = null;
        }

        value = rs.getString("path");
        FsPath path = value == null ? null : computeFsPath(null, value);

        Timestamp timestamp = rs.getTimestamp("started_at");
        Long startedAt = timestamp == null ? null : timestamp.getTime();

        String errorType = rs.getString("error_type");
        Throwable error = null;

        if (errorType != null) {
            /** this is a placeholder **/
            error = new Throwable(
                  "[errorType: " + errorType + "] " + rs.getString("error_message"));
        }

        return BulkRequestTargetBuilder.builder()
              .id(rs.getLong("id"))
              .pid(PID.values()[rs.getInt("pid")])
              .rid(rs.getLong("rid"))
              .state(State.valueOf(rs.getString("state")))
              .attributes(attributes)
              .path(path)
              .createdAt(rs.getTimestamp("created_at").getTime())
              .startedAt(startedAt)
              .lastUpdated(rs.getTimestamp("last_updated").getTime())
              .error(error).build();
    }

    public int update(JdbcRequestTargetCriterion criterion, JdbcRequestTargetUpdate update) {
        int count = 0;
        if (criterion.isJoined()) {
            count = utils.update(criterion, update, TABLE_NAME, SECONDARY_TABLE_NAME, this);
        } else
            count = utils.update(criterion, update, TABLE_NAME, this);
        statistics.increment(update.getStateName(), count);
        return count;
    }

    public JdbcRequestTargetCriterion where() {
        return new JdbcRequestTargetCriterion();
    }
}
