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
package org.dcache.qos.services.engine.data.db;

import diskCacheV111.util.PnfsId;
import java.util.List;
import java.util.Optional;
import org.dcache.qos.services.engine.data.QoSRecord;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

/**
 * Simple generic SQL.
 */
public class JdbcQoSEngineDao extends JdbcDaoSupport {

    private static final String INSERT = "INSERT INTO qos_file_status (pnfsid, expires, state) "
          + "VALUES (?,?,?)";

    private static final String UPDATE = "UPDATE qos_file_status SET expires=expires+?, state=? "
          + "WHERE pnfsid=? AND state!=?";

    private static final String DELETE = "DELETE FROM qos_file_status WHERE pnfsid=?";

    private static final String SELECT_EXPIRED =
          "SELECT id, pnfsid, expires, state FROM qos_file_status "
                + "WHERE expires <=? and id >=? ORDER BY id limit ?";

    private static final String SELECT = "SELECT id, pnfsid, expires, state FROM qos_file_status WHERE pnfsid=?";

    private static RowMapper<QoSRecord> QOS_RECORD_MAPPER = (rs, rowNum) -> {
        return new QoSRecord(rs.getLong(1), new PnfsId(rs.getString(2)),
              rs.getLong(3), rs.getInt(4));
    };

    private int fetchSize;

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public long findExpired(List<QoSRecord> expired, long offset, int limit) {
        long[] max = {0L};
        long now = System.currentTimeMillis();
        JdbcTemplate template = getJdbcTemplate();
        template.setFetchSize(fetchSize);
        template.query(SELECT_EXPIRED, ps -> {
            ps.setLong(1, now);
            ps.setLong(2, offset);
            ps.setInt(3, limit);
        }, rs -> {
            expired.add(QOS_RECORD_MAPPER.mapRow(rs, rs.getRow()));
            max[0] = Math.max(max[0], rs.getLong(1));
        });

        return max[0];
    }

    public Optional<QoSRecord> getRecord(PnfsId pnfs) {
        try {
            return Optional.ofNullable(
                  getJdbcTemplate().queryForObject(SELECT, QOS_RECORD_MAPPER,
                        pnfs.toString()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    /**
     * Generic (does not take advantage of the Postgres-specific ON CONFLICT);
     */
    public boolean upsert(PnfsId pnfsId, int index, long duration) {
        try {
            return 0 < getJdbcTemplate().update(INSERT, ps -> {
                ps.setString(1, pnfsId.toString());
                ps.setLong(2, System.currentTimeMillis() + duration);
                ps.setInt(3, index);
            });
        } catch (DuplicateKeyException e) {
            return 0 < getJdbcTemplate().update(UPDATE, ps -> {
                ps.setLong(1, duration);
                ps.setInt(2, index);
                ps.setString(3, pnfsId.toString());
                ps.setInt(4, index);
            });
        }
    }

    public boolean delete(PnfsId pnfsId) {
        try {
            return 0 < getJdbcTemplate().update(DELETE, ps -> {
                ps.setString(1, pnfsId.toString());
            });
        } catch (DataAccessException e) {
            return false;
        }
    }
}
