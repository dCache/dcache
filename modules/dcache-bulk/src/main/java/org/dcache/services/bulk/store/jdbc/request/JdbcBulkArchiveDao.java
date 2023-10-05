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

import com.google.common.base.Throwables;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import org.dcache.services.bulk.BulkArchivedRequestInfo;
import org.dcache.services.bulk.BulkArchivedSummaryInfo;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.store.jdbc.JdbcBulkDaoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

/**
 * CRUD for the request archive table (WORM).
 */
public final class JdbcBulkArchiveDao extends JdbcDaoSupport {

    public static final String TABLE_NAME = "request_archive";

    private static final String SELECT_INFO = "SELECT info";

    private static final String SELECT_SUMMARY = "SELECT uid, owner, last_modified, activity, status ";

    private static final String INSERT_ARCHIVED_REQUEST =
          "INSERT INTO " + TABLE_NAME + " (uid, owner, last_modified, activity, status, info) "
                + "VALUES (?, ?, ?, ?, ?, ?)";

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcBulkArchiveDao.class);

    class InfoRowMapper implements RowMapper<BulkArchivedRequestInfo> {
        @Override
        public BulkArchivedRequestInfo mapRow(ResultSet rs, int rowNum) throws SQLException {
            InputStream in = rs.getBinaryStream(1);
            try (ObjectInputStream istream = new ObjectInputStream(in)) {
                return (BulkArchivedRequestInfo) istream.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new SQLException("could not map row "
                      + rowNum + " of query result on " + TABLE_NAME);
            }
        }
    }

    class SummaryRowMapper implements RowMapper<BulkArchivedSummaryInfo> {
        @Override
        public BulkArchivedSummaryInfo mapRow(ResultSet rs, int rowNum) throws SQLException {
            BulkArchivedSummaryInfo info = new BulkArchivedSummaryInfo();
            info.setUid(rs.getString(1));
            info.setOwner(rs.getString(2));
            info.setLastModified(rs.getTimestamp(3).getTime());
            info.setActivity(rs.getString(4));
            info.setStatus(rs.getString(5));
            return info;
        }
    }

    private final InfoRowMapper infoRowMapper = new InfoRowMapper();
    private final SummaryRowMapper summaryRowMapper = new SummaryRowMapper();

    private JdbcBulkDaoUtils utils;

    public int count(JdbcArchivedBulkRequestCriterion criterion) {
        return utils.count(criterion, TABLE_NAME, this);
    }

    public int delete(JdbcArchivedBulkRequestCriterion criterion) {
        return utils.delete(criterion, TABLE_NAME, this);
    }

    public List<BulkArchivedRequestInfo> get(JdbcArchivedBulkRequestCriterion criterion, int limit) {
       return utils.get(SELECT_INFO, criterion, limit, TABLE_NAME, this, infoRowMapper);
    }

    public List<BulkArchivedSummaryInfo> list(JdbcArchivedBulkRequestCriterion criterion, int limit) {
        return utils.get(SELECT_SUMMARY, criterion, limit, TABLE_NAME, this, summaryRowMapper);
    }

    public void insert(BulkArchivedRequestInfo info) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream ostream = new ObjectOutputStream(baos)) {
                ostream.writeObject(info);
            } catch (IOException e) {
                throw new BulkStorageException("problem serializing request", e);
            }

            getJdbcTemplate().update(INSERT_ARCHIVED_REQUEST, ps -> {
                ps.setString(1, info.getUid());
                ps.setString(2, info.getOwner());
                ps.setTimestamp(3, new Timestamp(info.getLastModified()));
                ps.setString(4, info.getActivity());
                ps.setString(5, info.getStatus());
                ps.setBinaryStream(6, new ByteArrayInputStream(baos.toByteArray()));
            });
        } catch (BulkStorageException e) {
            LOGGER.error("Could not store info for {}: {}, cause {}.", info.getUid(),
                  Throwables.getRootCause(e));
        }
    }

    @Required
    public void setUtils(JdbcBulkDaoUtils utils) {
        this.utils = utils;
    }

    public JdbcArchivedBulkRequestCriterion where() {
        return new JdbcArchivedBulkRequestCriterion();
    }
}
