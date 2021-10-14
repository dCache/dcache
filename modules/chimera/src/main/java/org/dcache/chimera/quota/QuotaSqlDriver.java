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

package org.dcache.chimera.quota;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import javax.sql.DataSource;
import org.dcache.chimera.quota.spi.DbDriverProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

public class QuotaSqlDriver {

    /**
     * This class interacts with DB backend to manipulate quotas
     */

    private static final Logger LOGGER =
          LoggerFactory.getLogger(QuotaSqlDriver.class);

    private static final ServiceLoader<DbDriverProvider> ALL_PROVIDERS =
          ServiceLoader.load(DbDriverProvider.class);

    final JdbcTemplate jdbc;

    public QuotaSqlDriver(DataSource dataSource) {
        jdbc = new JdbcTemplate(dataSource);
    }

    public static QuotaSqlDriver getDriverInstance(DataSource dataSource)
          throws SQLException {
        for (DbDriverProvider driverProvider : ALL_PROVIDERS) {
            if (driverProvider.isSupportedDB(dataSource)) {
                QuotaSqlDriver driver = driverProvider.getDriver(dataSource);
                LOGGER.info("Using DBDriverProvider for Quota: {}", driver.getClass().getName());
                return driver;
            }
        }
        return new QuotaSqlDriver(dataSource);
    }

    private static final String UPDATE_USER_QUOTAS_SQL =
          "MERGE INTO t_user_quota " +
                "USING (SELECT " +
                "iuid, " +
                "SUM(CASE WHEN iretention_policy = 0 THEN isize ELSE 0 END) AS custodial, " +
                "SUM(CASE WHEN iretention_policy = 1 THEN isize ELSE 0 END) AS output, " +
                "SUM(CASE WHEN iretention_policy = 2 THEN isize ELSE 0 END) AS replica " +
                "FROM t_inodes WHERE itype=32768 " +
                "AND iuid IN (SELECT iuid FROM t_user_quota) " +
                "GROUP BY iuid) AS t(iuid, custodial, output, replica) " +
                "ON t.iuid = t_user_quota.iuid " +
                "WHEN MATCHED THEN UPDATE SET " +
                "t_user_quota.icustodial_used = t.custodial, " +
                "t_user_quota.ioutput_used = t.output, " +
                "t_user_quota.ireplica_used = t.replica " +
                "WHEN NOT MATCHED THEN INSERT " +
                "(iuid, icustodial_used, ioutput_used, ireplica_used) " +
                "VALUES (t.iuid, t.custodial, t.output, t.replica)";

    /**
     * Update user quotas
     */
    public void updateUserQuota() {
        try {
            jdbc.update(UPDATE_USER_QUOTAS_SQL);
        } catch (DataAccessException e) {
            LOGGER.error("Failed to update user quotas {}", e.getMessage());
        }
    }

    private static final String UPDATE_GROUP_QUOTAS_SQL =
          "MERGE INTO t_group_quota " +
                "USING (SELECT " +
                "igid, " +
                "SUM(CASE WHEN iretention_policy = 0 THEN isize ELSE 0 END) AS custodial, " +
                "SUM(CASE WHEN iretention_policy = 1 THEN isize ELSE 0 END) AS output, " +
                "SUM(CASE WHEN iretention_policy = 2 THEN isize ELSE 0 END) AS replica " +
                "FROM t_inodes WHERE itype=32768 " +
                "AND igid IN (SELECT igid FROM t_group_quota) " +
                "GROUP BY igid) AS t(igid, custodial, output, replica) " +
                "ON t.igid = t_group_quota.igid " +
                "WHEN MATCHED THEN UPDATE SET " +
                "t_group_quota.icustodial_used = t.custodial, " +
                "t_group_quota.ioutput_used = t.output, " +
                "t_group_quota.ireplica_used = t.replica " +
                "WHEN NOT MATCHED THEN INSERT " +
                "(igid, icustodial_used, ioutput_used, ireplica_used) " +
                "VALUES (t.igid, t.custodial, t.output, t.replica)";

    /**
     * Update group quotas
     */
    public void updateGroupQuota() {
        try {
            jdbc.update(UPDATE_GROUP_QUOTAS_SQL);
        } catch (DataAccessException e) {
            LOGGER.error("Failed to update group quotas {}", e.getMessage());
        }
    }

    private static final String SELECT_USER_QUOTAS_SQL =
          "SELECT iuid, " +
                "icustodial_used, icustodial_limit, " +
                "ioutput_used, ioutput_limit, " +
                "ireplica_used, ireplica_limit " +
                "FROM t_user_quota";


    private Long bigDecimalToLong(BigDecimal val) {
        return val == null ? null :
              val.min(BigDecimal.valueOf(Long.MAX_VALUE)).longValue();
    }

    public Map<Integer, Quota> getUserQuotas() {
        Map<Integer, Quota> quotas = new HashMap<>();
        jdbc.query(SELECT_USER_QUOTAS_SQL,
              (rs) -> {
                  int id = rs.getInt("iuid");
                  quotas.put(id,
                        new Quota(id,
                              rs.getLong("icustodial_used"),
                              bigDecimalToLong(rs.getBigDecimal("icustodial_limit")),
                              rs.getLong("ioutput_used"),
                              bigDecimalToLong(rs.getBigDecimal("ioutput_limit")),
                              rs.getLong("ireplica_used"),
                              bigDecimalToLong(rs.getBigDecimal("ireplica_limit"))));
              });
        LOGGER.debug("getUserQuotas, found {} records.", quotas.size());
        return quotas;
    }

    private static final String SELECT_GROUP_QUOTAS_SQL =
          "SELECT igid, " +
                "icustodial_used, icustodial_limit, " +
                "ioutput_used, ioutput_limit, " +
                "ireplica_used, ireplica_limit " +
                "FROM t_group_quota";


    public Map<Integer, Quota> getGroupQuotas() {
        Map<Integer, Quota> quotas = new HashMap<>();
        jdbc.query(SELECT_GROUP_QUOTAS_SQL,
              (rs) -> {
                  int id = rs.getInt("igid");
                  quotas.put(id, new Quota(id,
                        rs.getLong("icustodial_used"),
                        bigDecimalToLong(rs.getBigDecimal("icustodial_limit")),
                        rs.getLong("ioutput_used"),
                        bigDecimalToLong(rs.getBigDecimal("ioutput_limit")),
                        rs.getLong("ireplica_used"),
                        bigDecimalToLong(rs.getBigDecimal("ireplica_limit"))));
              });
        LOGGER.debug("getGroupQuotas, found {} records.", quotas.size());
        return quotas;
    }

    private static final String DELETE_USER_QUOTA = "DELETE FROM t_user_quota WHERE iuid = ?";

    public void deleteUserQuota(int uid) {
        jdbc.update(DELETE_USER_QUOTA, uid);
    }

    private static final String DELETE_GROUP_QUOTA = "DELETE FROM t_group_quota WHERE igid = ?";

    public void deleteGroupQuota(int gid) {
        jdbc.update(DELETE_GROUP_QUOTA, gid);
    }

    private static final String UPDATE_USER_QUOTA_SQL =
          "UPDATE t_user_quota SET " +
                "icustodial_limit = ?, ioutput_limit = ?, ireplica_limit=? " +
                "WHERE iuid = ?";

    public void setUserQuota(Quota q) {
        setQuota(UPDATE_USER_QUOTA_SQL, q);
    }

    private static final String UPDATE_GROUP_QUOTA_SQL =
          "UPDATE t_group_quota SET " +
                "icustodial_limit = ?, ioutput_limit = ?, ireplica_limit=? " +
                "WHERE igid = ?";

    public void setGroupQuota(Quota q) {
        setQuota(UPDATE_GROUP_QUOTA_SQL, q);
    }

    public void setQuota(String query, Quota q) {
        jdbc.update(query,
              q.getCustodialSpaceLimit(),
              q.getOutputSpaceLimit(),
              q.getReplicaSpaceLimit(),
              q.getId());
    }

    private static final String INSERT_USER_QUOTA =
          "INSERT INTO t_user_quota (iuid, icustodial_used, ioutput_used, ireplica_used, " +
                "icustodial_limit, ioutput_limit, ireplica_limit) " +
                "VALUES (?, 0, 0, 0, ?, ?, ?)";

    public void createUserQuota(Quota q) {
        createQuota(INSERT_USER_QUOTA, q);
    }

    private static final String INSERT_GROUP_QUOTA =
          "INSERT INTO t_group_quota (igid, icustodial_used, ioutput_used, ireplica_used, " +
                "icustodial_limit, ioutput_limit, ireplica_limit) " +
                "VALUES (?, 0, 0, 0, ?, ?, ?)";

    public void createGroupQuota(Quota q) {
        createQuota(INSERT_GROUP_QUOTA, q);
    }

    private void createQuota(String query, Quota q) {
        jdbc.update(query,
              q.getId(),
              q.getCustodialSpaceLimit(),
              q.getOutputSpaceLimit(),
              q.getReplicaSpaceLimit());

    }
}
