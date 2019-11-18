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
package org.dcache.resilience.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;

import org.dcache.auth.Subjects;
import org.dcache.chimera.BackEndErrorHimeraFsException;
import org.dcache.chimera.IOHimeraFsException;
import org.dcache.resilience.data.FileOperationMap;
import org.dcache.resilience.data.FileUpdate;
import org.dcache.resilience.data.MessageType;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.handlers.FileOperationHandler;
import org.dcache.resilience.handlers.PoolOperationHandler;
import org.dcache.resilience.util.ExceptionMessage;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.util.SqlHelper.tryToClose;

/**
 * <p>Provides handling of specialized resilience-related queries which require
 *      direct access to the underlying database. </p>
 *
 * <p>The {@link #handlePnfsidsForPool} uses a callback to
 *      the {@link FileOperationHandler} to add
 *      an entry in the pnfsid operation tables for each pnfsid.</p>
 *
 * <p>Class is not marked final so that a test version can be
 *      implemented by extension.</p>
 *
 * <p>Class is not marked final for the purpose of test extension.</p>
 */
public class LocalNamespaceAccess implements NamespaceAccess {
    static final String SQL_GET_ONLINE_FOR_LOCATION
                    = "SELECT n.ipnfsid FROM t_locationinfo l, t_inodes n "
                                    + "WHERE l.inumber = n.inumber "
                                    + "AND l.itype = 1 AND n.iaccess_latency = 1 "
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

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalNamespaceAccess.class);

    /**
     * <p>Handler for processing file operations.</p>
     */
    protected FileOperationHandler handler;

    /**
     * <p>Database connection pool for queries returning multiple pnfsid
     *      info.  This may be independent of the main pool
     *      for the namespace (in embedded mode), or may be shared
     *      (in standalone mode).</p>
     */
    private DataSource connectionPool;

    /**
     * <p>Delegate service used to extract file attributes.</p>
     */
    private NameSpaceProvider namespace;

    /**
     * <p>Round-trip buffer used when running pool-based queries.</p>
     */
    private int fetchSize;

    @Override
    public void printInaccessibleFiles(String location,
                                       PoolInfoMap poolInfoMap,
                                       PrintWriter printWriter)
                    throws CacheException, InterruptedException {
        try {
            Connection connection = getConnection();
            try {
                printResults(connection, location, poolInfoMap, printWriter);
            } catch (SQLException e) {
                throw new IOHimeraFsException(e.getMessage());
            } finally {
                tryToClose(connection);
            }
        } catch (IOHimeraFsException e) {
            throw new CacheException(CacheException.RESOURCE,
                            String.format("Could not handle pnfsids for %s",
                                            location), e);
        }
    }

    @Override
    public void printContainedInFiles(List<String> locations,
                                      PrintWriter printWriter)
                    throws CacheException, InterruptedException {
        try {
            Connection connection = getConnection();
            try {
                printResults(connection, locations, printWriter);
            } catch (SQLException e) {
                throw new IOHimeraFsException(e.getMessage());
            } finally {
                tryToClose(connection);
            }
        } catch (IOHimeraFsException e) {
            throw new CacheException(CacheException.RESOURCE,
                                     String.format("Could not handle pnfsids for %s",
                                                   locations), e);
        }
    }

    @Override
    public FileAttributes getRequiredAttributes(PnfsId pnfsId)
                    throws CacheException {
        return namespace.getFileAttributes(Subjects.ROOT,
                                           pnfsId,
                                           REQUIRED_ATTRIBUTES);
    }

    @Override
    public FileAttributes getRequiredAttributesForStaging(PnfsId pnfsId)
                    throws CacheException {
        return namespace.getFileAttributes(Subjects.ROOT,
                                           pnfsId,
                                           PoolMgrSelectReadPoolMsg.getRequiredAttributes());
    }

    /**
     * <p>Called by {@link PoolOperationHandler#handlePoolScan(ScanSummary)}.</p>
     */
    @Override
    public void handlePnfsidsForPool(ScanSummary scan)
                    throws CacheException {
        try {
            Connection connection = getConnection();
            try {
                handleQuery(connection, scan);
            } catch (SQLException | CacheException e) {
                throw new IOHimeraFsException(e.getMessage());
            } finally {
                tryToClose(connection);
            }
        } catch (IOHimeraFsException e) {
            throw new CacheException(CacheException.RESOURCE,
                                     String.format("Could not handle pnfsids for %s",
                                                   scan.getPool()), e);
        }
    }

    @Override
    public void refreshAttributes(FileAttributes attributes)
                    throws CacheException {
        FileAttributes refreshed =
                        namespace.getFileAttributes(Subjects.ROOT,
                                                    attributes.getPnfsId(),
                                                    REFRESHABLE_ATTRIBUTES);
        attributes.setLocations(refreshed.getLocations());
        attributes.setRetentionPolicy(refreshed.getRetentionPolicy());
        attributes.setAccessLatency(refreshed.getAccessLatency());
        attributes.setAccessTime(refreshed.getAccessTime());
    }

    @Override
    public void setConnectionPool(DataSource connectionPool) {
        this.connectionPool = connectionPool;
    }

    @Override
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public void setHandler(FileOperationHandler handler) {
        this.handler = handler;
    }

    @Override
    public void setNamespace(NameSpaceProvider namespace) {
        this.namespace = namespace;
    }

    private Connection getConnection() throws IOHimeraFsException {
        try {
            return connectionPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }
    }

    /**
     * <p>The query processes all pnfsids for the given location which
     *      have access latency = ONLINE.  These are sent one-by-one to the
     *      {@link FileOperationHandler} to either create or update a
     *      corresponding entry in the {@link FileOperationMap}.</p>
     */
    private void handleQuery(Connection connection, ScanSummary scan)
                    throws SQLException, CacheException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String pool = scan.getPool();
        MessageType type = scan.getType();
        Integer group = scan.getGroup();
        Integer storageUnit = scan.getStorageUnit();
        boolean full = scan.isForced();

        try {
            statement = connection.prepareStatement(SQL_GET_ONLINE_FOR_LOCATION);
            statement.setString(1, pool);
            statement.setFetchSize(fetchSize);
            if (scan.isCancelled()) {
                return;
            }

            resultSet = statement.executeQuery();

            while (resultSet.next() && !scan.isCancelled()) {
                PnfsId pnfsId = new PnfsId(resultSet.getString(1));
                FileUpdate data = new FileUpdate(pnfsId, pool, type, group, full);
                try {
                    LOGGER.debug("checking {}, {}.", pool, pnfsId);
                    if (handler.handleScannedLocation(data, storageUnit)) {
                        scan.incrementCount();
                    }
                    LOGGER.debug("after checking {}, {}, count is {}.", pool, pnfsId, scan.getCount());
                } catch (CacheException e) {
                    LOGGER.debug("{}: {}", data, new ExceptionMessage(e));
                }
            }
        } finally {
            tryToClose(resultSet);
            tryToClose(statement);
        }
    }

    /**
     * <p>Used by the inaccessible file query.</p>
     *
     * <p>Log at info level so that progress is visible in pinboard.</p>
     */
    private void printResults(Connection connection,
                    String location,
                    PoolInfoMap poolInfoMap,
                    PrintWriter writer)
                    throws SQLException, InterruptedException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            statement = connection.prepareStatement(SQL_GET_ONLINE_FOR_LOCATION);
            statement.setString(1, location);
            statement.setFetchSize(fetchSize);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            LOGGER.info("executing {}.", statement);
            resultSet = statement.executeQuery();

            LOGGER.info("starting check of pnfsids for {}.", location);

            while (resultSet.next()) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }

                PnfsId pnfsId = new PnfsId(resultSet.getString(1));
                try {
                    FileAttributes attributes = getRequiredAttributes(pnfsId);
                    Collection<String> locations = attributes.getLocations();
                    for (Iterator<String> i = locations.iterator(); i.hasNext();) {
                        String pool = i.next();
                        try {
                            int index = poolInfoMap.getPoolIndex(pool);
                            if (!poolInfoMap.isPoolViable(index, false)) {
                                i.remove();
                            }
                        } catch (NoSuchElementException e) {
                            i.remove();
                        }
                    }
                    if (locations.isEmpty()) {
                        writer.println(pnfsId);
                        LOGGER.info("orphaned: {}.", pnfsId);
                    }
                } catch (CacheException e) {
                    LOGGER.info("{}: {}", pnfsId, new ExceptionMessage(e));
                }
            }
        } finally {
            tryToClose(resultSet);
            tryToClose(statement);
        }

        LOGGER.info("Printing of inaccessible files for {} completed.", location);
    }

    private void printResults(Connection connection,
                              List<String> locations,
                              PrintWriter writer)
                    throws SQLException, InterruptedException {
        String placeholders = String.join(",", locations.stream()
                        .map(l -> "?").collect(Collectors.toList()));

        String query = String.format(SQL_GET_CONTAINED_IN,
                                     placeholders,
                                     placeholders);

        PreparedStatement statement = null;
        ResultSet resultSet = null;
        int len = locations.size();

        try {
            statement = connection.prepareStatement(query);
            for (int i = 1; i <= len; ++i) {
                statement.setString(i, locations.get(i-1));
                statement.setString(i+len, locations.get(i-1));
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
