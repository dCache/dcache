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
package org.dcache.qos.services.scanner.namespace;

import static org.dcache.qos.data.QoSMessageType.CHECK_CUSTODIAL_ONLINE;
import static org.dcache.util.SqlHelper.tryToClose;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.dcache.cells.CellStub;
import org.dcache.chimera.BackEndErrorChimeraFsException;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.listeners.QoSVerificationListener;
import org.dcache.qos.services.scanner.data.PoolScanSummary;
import org.dcache.qos.util.CacheExceptionUtils;
import org.dcache.qos.vehicles.QoSScannerVerificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides handling of specialized long-running queries which require direct access to the
 * underlying namespace database.
 * <p/>
 * Class is not marked final so that a test version can be implemented by extension.
 */
public class LocalNamespaceAccess implements NamespaceAccess {

    static final String SQL_GET_ALL_FOR_LOCATION
          = "SELECT n.ipnfsid FROM t_locationinfo l, t_inodes n "
          + "WHERE l.inumber = n.inumber "
          + "AND l.itype = 1 "
          + "AND l.ilocation = ?";

    static final String SQL_GET_CONTAINED_IN
          = "SELECT n.ipnfsid FROM t_locationinfo l, t_inodes n "
          + "WHERE n.inumber = l.inumber "
          + "AND l.ilocation IN (%s) "
          + "AND NOT EXISTS "
          + "(SELECT n1.ipnfsid FROM t_locationinfo l1, t_inodes n1 "
          + "WHERE n.inumber = l1.inumber "
          + "AND n.inumber = n1.inumber "
          + "AND l1.ilocation NOT IN (%s))";

    static final String SQL_MISSING_ONLINE_CUSTODIAL
          = "SELECT n.ipnfsid FROM t_inodes n, t_locationinfo l0 "
          + "WHERE n.inumber=l0.inumber "
          + "AND l0.itype=0 "
          + "AND n.iaccess_latency=1 "
          + "AND n.iretention_policy=0 "
          + "AND NOT EXISTS "
          + "(SELECT * FROM t_locationinfo l1 "
          + "WHERE n.inumber=l1.inumber "
          + "AND l1.itype=1)";

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalNamespaceAccess.class);

    /**
     * Callback to service for sending notifications.
     */
    protected QoSVerificationListener verificationListener;

    /**
     * Database connection pool for queries returning multiple pnfsid info.
     */
    private DataSource connectionPool;

    /**
     * Delegate service used to extract file attributes; this is used when printing the results of
     * the inaccessible or contained-in queries.
     */
    private NameSpaceProvider namespace;

    /**
     * Round-trip buffer used when running pool-based queries.
     */
    private int fetchSize;

    /**
     * Endpoint used for verifying replicas.
     */
    private CellStub pools;

    @Override
    public void printContainedInFiles(List<String> locations, PrintWriter printWriter)
          throws CacheException, InterruptedException {
        try {
            Connection connection = getConnection();
            try {
                printResults(connection, locations, printWriter);
            } catch (SQLException e) {
                throw new ChimeraFsException(e.getMessage());
            } finally {
                tryToClose(connection);
            }
        } catch (ChimeraFsException e) {
            throw new CacheException(CacheException.RESOURCE,
                  String.format("Could not handle pnfsids for %s",
                        locations), e);
        }
    }

    @Override
    public void handlePnfsidsForPool(PoolScanSummary scan) throws CacheException {
        try {
            Connection connection = getConnection();
            try {
                handleQuery(connection, scan);
            } catch (SQLException e) {
                throw new ChimeraFsException(e.getMessage());
            } catch (QoSException e) {
                throw CacheExceptionUtils.getCacheExceptionFrom(e);
            } finally {
                tryToClose(connection);
            }
        } catch (ChimeraFsException e) {
            throw new CacheException(CacheException.RESOURCE,
                  String.format("Could not handle pnfsids for %s",
                        scan.getId()), e);
        }
    }

    @Override
    public void setConnectionPool(DataSource connectionPool) {
        this.connectionPool = connectionPool;
    }

    @Override
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    @Override
    public void setNamespace(NameSpaceProvider namespace) {
        this.namespace = namespace;
    }

    public void setPools(CellStub pools) {
        this.pools = pools;
    }

    public void setVerificationListener(QoSVerificationListener verificationListener) {
        this.verificationListener = verificationListener;
    }

    private Connection getConnection() throws ChimeraFsException {
        try {
            return connectionPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorChimeraFsException(e.getMessage());
        }
    }

    /**
     * The query processes all replicas for the location by batching the returned pnfsids into lists
     * to be dispatched to the verification service.
     */
    private void handleQuery(Connection connection, PoolScanSummary scan)
          throws SQLException, QoSException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String pool = scan.getId();
        String group = scan.getGroup();
        String storageUnit = scan.getStorageUnit();
        QoSMessageType type = scan.getType();
        boolean forced = scan.isForced();
        List<PnfsId> replicas = new ArrayList<>();
        QoSScannerVerificationRequest request;

        LOGGER.debug("handleQuery: (pool {})(group {})(storageUnit {})(type {})(forced {})",
              pool, group, storageUnit, type, forced);

        try {
            if (CHECK_CUSTODIAL_ONLINE == type) {
                statement = connection.prepareStatement(SQL_MISSING_ONLINE_CUSTODIAL);
            } else {
                statement = connection.prepareStatement(SQL_GET_ALL_FOR_LOCATION);
                statement.setString(1, pool);
            }
            statement.setFetchSize(fetchSize);
            if (scan.isCancelled()) {
                return;
            }

            resultSet = statement.executeQuery();

            while (resultSet.next() && !scan.isCancelled()) {
                replicas.add(new PnfsId(resultSet.getString(1)));
                scan.incrementCount();
                if (replicas.size() >= fetchSize) {
                    request = new QoSScannerVerificationRequest(pool, replicas, type, group,
                          storageUnit, forced);
                    verificationListener.fileQoSVerificationRequested(request);
                    replicas = new ArrayList<>();
                }
            }

            if (!replicas.isEmpty() && !scan.isCancelled()) {
                request = new QoSScannerVerificationRequest(pool, replicas, type, group,
                      storageUnit, forced);
                verificationListener.fileQoSVerificationRequested(request);
            }
        } finally {
            tryToClose(resultSet);
            tryToClose(statement);
        }
    }

    private void printResults(Connection connection, List<String> locations, PrintWriter writer)
          throws SQLException, InterruptedException {
        String placeholders = locations.stream().map(l -> "?")
              .collect(Collectors.joining(","));

        String query = String.format(SQL_GET_CONTAINED_IN,
              placeholders,
              placeholders);

        PreparedStatement statement = null;
        ResultSet resultSet = null;
        int len = locations.size();

        try {
            statement = connection.prepareStatement(query);
            for (int i = 1; i <= len; ++i) {
                statement.setString(i, locations.get(i - 1));
                statement.setString(i + len, locations.get(i - 1));
            }
            statement.setFetchSize(fetchSize);

            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            LOGGER.info("executing {}.", statement);
            resultSet = statement.executeQuery();

            LOGGER.info("starting check of pnfsids for {}.", locations);

            while (resultSet.next()) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }

                PnfsId pnfsId = new PnfsId(resultSet.getString(1));
                writer.println(pnfsId);
            }
        } finally {
            tryToClose(resultSet);
            tryToClose(statement);
        }

        LOGGER.info("Printing of contained files for {} completed.", locations);
    }
}
