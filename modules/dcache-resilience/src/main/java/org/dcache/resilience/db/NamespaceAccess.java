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

import javax.sql.DataSource;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;

import org.dcache.namespace.FileAttribute;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.vehicles.FileAttributes;

/**
 * <p>Specialized namespace API for resilience handling.</p>
 */
public interface NamespaceAccess {
    Set<FileAttribute> REQUIRED_ATTRIBUTES
                    = Collections.unmodifiableSet
                    (EnumSet.of(FileAttribute.PNFSID,
                                FileAttribute.SIZE,
                                FileAttribute.ACCESS_TIME,
                                FileAttribute.ACCESS_LATENCY,
                                FileAttribute.RETENTION_POLICY,
                                FileAttribute.STORAGECLASS,
                                FileAttribute.HSM,
                                FileAttribute.LOCATIONS));

    Set<FileAttribute> REFRESHABLE_ATTRIBUTES
                    = Collections.unmodifiableSet(
                    EnumSet.of(FileAttribute.ACCESS_TIME,
                               FileAttribute.RETENTION_POLICY,
                               FileAttribute.ACCESS_LATENCY,
                               FileAttribute.LOCATIONS));

    /**
     * <p>Pass-through to namespace.</p>
     *
     * <p>Requests attributes defined by #REQUIRED_ATTRIBUTES.</p>
     */
    FileAttributes getRequiredAttributes(PnfsId pnfsId) throws CacheException;

    /**
     * <p>Pass-through to namespace.</p>
     *
     * <p>Requests attributes defined by {@link PoolMgrSelectReadPoolMsg#getRequiredAttributes()}.</p>
     */
    FileAttributes getRequiredAttributesForStaging(PnfsId pnfsId) throws CacheException;

    /**
     * <p>The workhorse query.
     */
    void handlePnfsidsForPool(ScanSummary scan) throws CacheException;


    /**
     * <p>Used by the admin command to create a file of all the pnfsids
     *    on a pool which currently have no readable locations.</p>
     *
     * @param location pool name.
     * @param poolInfoMap for checking location readability.
     * @param printWriter to write the results to.
     */
    void printInaccessibleFiles(String location,
                                PoolInfoMap poolInfoMap,
                                PrintWriter printWriter)
                    throws CacheException, InterruptedException;

    void printContainedInFiles(List<String> locations,
                               PrintWriter printWriter)
                    throws CacheException, InterruptedException;

    /**
     * <p>Pass-through to namespace.</p>
     *
     * <p>Requests attributes defined by #REFRESHABLE_ATTRIBUTES and
     *      resets them on the passed in attributes.</p>
     */
    void refreshAttributes(FileAttributes attributes) throws CacheException;

    void setConnectionPool(DataSource connectionPool);

    void setFetchSize(int fetchSize);

    void setNamespace(NameSpaceProvider namespace);
}
