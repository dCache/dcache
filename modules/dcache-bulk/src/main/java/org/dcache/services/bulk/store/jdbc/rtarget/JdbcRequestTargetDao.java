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

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import java.math.BigDecimal;
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
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.services.bulk.util.BulkRequestTargetBuilder;
import org.dcache.vehicles.FileAttributes;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.KeyHolder;

/**
 * CRUD for the request target table.
 */
public final class JdbcRequestTargetDao extends JdbcDaoSupport {

    static class TargetPlaceholder {
        String rid;
        String path;
        String activity;
        String state;
    }

    static final String TABLE_NAME = "request_target";

    static final String BATCH_INSERT = "INSERT INTO " + TABLE_NAME + " ("
          + "pid, rid, pnfsid, path, type, activity, state, created_at, last_updated) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    static final ParameterizedPreparedStatementSetter<TargetPlaceholder> SETTER = (ps, target) -> {
        Instant now = Instant.now();
        ps.setBigDecimal(1, BigDecimal.ZERO); // DEPRECATED, will be removed REVISIT
        ps.setString(2, target.rid);
        ps.setString(3, "?");
        ps.setString(4, target.path);
        ps.setString(5, "?");
        ps.setString(6, target.activity);
        ps.setString(7, target.state);
        ps.setTimestamp(8, Timestamp.from(now));
        ps.setTimestamp(9, Timestamp.from(now));
    };

    private JdbcBulkDaoUtils utils;

    public int count(JdbcRequestTargetCriterion criterion) {
        return utils.count(criterion, TABLE_NAME, this);
    }

    public Map<String, Long> count(JdbcRequestTargetCriterion criterion, String classifier) {
        return utils.countGrouped(criterion.classifier(classifier), TABLE_NAME, this);
    }

    public Map<String, Long> countStates() {
        return utils.countGrouped(where().classifier("state"), TABLE_NAME, this);
    }

    public int delete(JdbcRequestTargetCriterion criterion) {
        return utils.delete(criterion, TABLE_NAME, this);
    }

    public List<BulkRequestTarget> get(JdbcRequestTargetCriterion criterion, int limit) {
        return utils.get(criterion, limit, TABLE_NAME, this, this::toRequestTarget);
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
            t.activity = request.getActivity();
            String path = target.trim();
            if (path.isEmpty()) {
                t.path = "invalid (empty) path";
                t.state = FAILED.name();
            } else {
                t.path = target;
                t.state = CREATED.name();
            }
            if (seen.contains(t.path)) {
                continue;
            }
            seen.add(t.path);
            targets.add(t);
        }
        utils.insertBatch(targets, BATCH_INSERT, SETTER, this);
    }

    public Optional<KeyHolder> insertOrUpdate(JdbcRequestTargetUpdate update) {
        return utils.upsert(update.getInsertOrUpdateSql(), update.getArguments(), this);
    }

    public JdbcRequestTargetUpdate set() {
        return new JdbcRequestTargetUpdate();
    }

    @Required
    public void setUtils(JdbcBulkDaoUtils utils) {
        this.utils = utils;
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
        String errorMessage = rs.getString("error_message");

        return BulkRequestTargetBuilder.builder()
              .id(rs.getLong("id"))
              .pid(rs.getLong("pid"))
              .rid(rs.getString("rid"))
              .activity(rs.getString("activity"))
              .state(State.valueOf(rs.getString("state")))
              .attributes(attributes)
              .path(path)
              .createdAt(rs.getTimestamp("created_at").getTime())
              .startedAt(startedAt)
              .lastUpdated(rs.getTimestamp("last_updated").getTime())
              .errorType(errorType).errorMessage(errorMessage).build();
    }

    public int update(JdbcRequestTargetCriterion criterion, JdbcRequestTargetUpdate update) {
        return utils.update(criterion, update, TABLE_NAME, this);
    }

    public JdbcRequestTargetCriterion where() {
        return new JdbcRequestTargetCriterion();
    }
}
