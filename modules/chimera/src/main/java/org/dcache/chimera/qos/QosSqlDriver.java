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
package org.dcache.chimera.qos;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import javax.sql.DataSource;
import org.dcache.chimera.qos.spi.DbDriverProvider;
import org.dcache.qos.DefaultQoSPolicyJsonDeserializer;
import org.dcache.qos.QoSPolicy;
import org.dcache.qos.QoSPolicyStat;
import org.dcache.qos.QoSPolicyStateCount;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class QosSqlDriver {
    protected static final Logger LOGGER = LoggerFactory.getLogger(QosSqlDriver.class);

    public static QosSqlDriver getDriverInstance(DataSource dataSource)
          throws SQLException {
        for (DbDriverProvider driverProvider : ALL_PROVIDERS) {
            if (driverProvider.isSupportedDB(dataSource)) {
                QosSqlDriver driver = driverProvider.getDriver(dataSource);
                LOGGER.info("Using DBDriverProvider for Quota: {}", driver.getClass().getName());
                return driver;
            }
        }
        return new QosSqlDriver(dataSource);
    }

    private static final ServiceLoader<DbDriverProvider> ALL_PROVIDERS =
          ServiceLoader.load(DbDriverProvider.class);

    private final static RowMapper<String> POLICY_NAME_MAPPER = (rs, rownum) -> {
        return rs.getString(1);
    };

    final JdbcTemplate jdbc;

    public QosSqlDriver(DataSource dataSource) {
        jdbc = new JdbcTemplate(dataSource);
    }

    private static final String INSERT_QOS_POLICY_SQL
          = "INSERT INTO t_qos_policy (name, policy) VALUES (?, ?)";

    private static final String SELECT_QOS_POLICY_SQL
          = "SELECT policy FROM t_qos_policy WHERE name=?";

    private static final String SELECT_QOS_POLICY_ID_SQL
          = "SELECT id FROM t_qos_policy WHERE name=?";

    private static final String SELECT_QOS_POLICY_NAMES_SQL
          = "SELECT name FROM t_qos_policy ORDER BY name";

    private static final String DELETE_QOS_POLICY_SQL
          = "DELETE FROM t_qos_policy WHERE name=?";

    private static final String SELECT_QOS_POLICY_COUNTS
          = "SELECT p.name, n.iqos_state, count(n) FROM t_inodes n, t_qos_policy p "
          + "WHERE n.iqos_policy = p.id  AND n.iqos_state >= 0 GROUP BY p.name, n.iqos_state";

    private static final String INVALIDATE_QOS_POLICY
          = "UPDATE t_inodes SET iqos_policy = NULL, iqos_state = NULL WHERE iqos_policy = ?";

    public boolean insertPolicy(QoSPolicy policy) {
        try {
            return 0 < jdbc.update(INSERT_QOS_POLICY_SQL, ps -> {
                ps.setString(1, policy.getName());
                ps.setString(2, new JSONObject(policy).toString());
            });
        } catch (DataAccessException e) {
            LOGGER.error("Failed to insert qos policy {}, {}", policy.getName(), e.getMessage());
            return false;
        }
    }

    public boolean deletePolicy(String policyName) {
        try {
            return 0 < jdbc.update(DELETE_QOS_POLICY_SQL, ps-> {
                ps.setString(1, policyName);
            });
        } catch (DataAccessException e) {
            LOGGER.error("Failed to delete qos policy {}, {}", policyName, e.getMessage());
        }
        return false;
    }

    public QoSPolicy selectPolicy(String policyName) {
        Map<String, QoSPolicy> policyHolder = new HashMap<>();
        try {
            jdbc.query(SELECT_QOS_POLICY_SQL,
                  ps -> {
                      ps.setString(1, policyName);
                  },
                  rs -> {
                      policyHolder.put(policyName,
                            DefaultQoSPolicyJsonDeserializer.fromJsonString(rs.getString(1)));
                  });
        } catch (DataAccessException e) {
            LOGGER.error("Failed to select qos policy {}, {}", policyName, e.getMessage());
        }

        return policyHolder.get(policyName);
    }

    public int getPolicyId(String policyName) throws DataAccessException {
        int[] id = new int[1];
        jdbc.query(SELECT_QOS_POLICY_ID_SQL,
              ps -> {
                  ps.setString(1, policyName);
              },
              rs -> {
                  id[0] = rs.getInt(1);
              });
        return id[0];
    }

    public List<QoSPolicyStat> selectCounts() {
        Multimap<String, QoSPolicyStateCount> map = HashMultimap.create();

        try {
            jdbc.query(SELECT_QOS_POLICY_COUNTS,
                  rs -> {
                      String name = rs.getString(1);
                      QoSPolicyStateCount count = new QoSPolicyStateCount(rs.getInt(2),
                            rs.getLong(3));
                      map.put(name, count);
                  });
        } catch (DataAccessException e) {
            LOGGER.error("Failed to select qos policy counts: {}", e.getMessage());
        }

        List<QoSPolicyStat> stats = new ArrayList<>();
        map.asMap().entrySet().forEach(e -> {
            QoSPolicyStat stat = new QoSPolicyStat(e.getKey());
            stat.getStateCountsList().addAll(e.getValue());
            stats.add(stat);
        });

        return stats;
    }

    public List<String> selectPolicyNames() {
        try {
            return  jdbc.query(SELECT_QOS_POLICY_NAMES_SQL, POLICY_NAME_MAPPER);
        } catch (DataAccessException e) {
            LOGGER.error("Failed to select qos policy names: {}", e.getMessage());
            return List.of();
        }
    }

    public void invalidateQosPolicy(Integer id) {
        jdbc.update(INVALIDATE_QOS_POLICY, ps -> ps.setInt(1, id));
    }
}
