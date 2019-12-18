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
package org.dcache.pool.json;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.dcache.cells.json.CellData;
import org.dcache.pool.PoolInfoRequestHandler;
import org.dcache.pool.classic.json.ChecksumModuleData;
import org.dcache.pool.classic.json.FlushControllerData;
import org.dcache.pool.classic.json.HSMFlushQManagerData;
import org.dcache.pool.classic.json.JobTimeoutManagerData;
import org.dcache.pool.classic.json.SweeperData;
import org.dcache.pool.classic.json.TransferServicesData;
import org.dcache.pool.migration.json.MigrationData;
import org.dcache.pool.nearline.json.StorageHandlerData;
import org.dcache.pool.p2p.json.P2PData;
import org.dcache.pool.repository.json.RepositoryData;

/**
 * <p>Top level container for data concerning the pool, processed by
 * {@link PoolInfoRequestHandler}.</p>
 */
public class PoolData implements Serializable {
    private static final long serialVersionUID = 7883809367201783768L;

    private List<String>          poolGroups = new ArrayList<>();
    private Set<String>           links = new HashSet<>();
    private CellData              cellData = new CellData();
    private ChecksumModuleData    csmData = new ChecksumModuleData();
    private PoolDataDetails       detailsData = new PoolDataDetails();
    private FlushControllerData   flushData = new FlushControllerData();
    private HSMFlushQManagerData  hsmFlushQMData = new HSMFlushQManagerData();
    private JobTimeoutManagerData jtmData = new JobTimeoutManagerData();
    private MigrationData         migrationData = new MigrationData();
    private P2PData               ppData = new P2PData();
    private RepositoryData        repositoryData = new RepositoryData();
    private StorageHandlerData    storageHandlerData = new StorageHandlerData();
    private TransferServicesData  transferServicesData = new TransferServicesData();
    private SweeperData           sweeperData = new SweeperData();

    public CellData getCellData() {
        return cellData;
    }

    public ChecksumModuleData getCsmData() {
        return csmData;
    }

    public PoolDataDetails getDetailsData() {
        return detailsData;
    }

    public FlushControllerData getFlushData() {
        return flushData;
    }

    public HSMFlushQManagerData getHsmFlushQMData() {
        return hsmFlushQMData;
    }

    public JobTimeoutManagerData getJtmData() {
        return jtmData;
    }

    public Set<String> getLinks() {
        return links;
    }

    public MigrationData getMigrationData() {
        return migrationData;
    }

    public List<String> getPoolGroups() {
        return poolGroups;
    }

    public P2PData getPpData() {
        return ppData;
    }

    public RepositoryData getRepositoryData() {
        return repositoryData;
    }

    public StorageHandlerData getStorageHandlerData() {
        return storageHandlerData;
    }

    public SweeperData getSweeperData() {
        return sweeperData;
    }

    public TransferServicesData getTransferServicesData() {
        return transferServicesData;
    }

    public void setCellData(CellData cellData) {
        this.cellData = cellData;
    }

    public void setCsmData(ChecksumModuleData csmData) {
        this.csmData = csmData;
    }

    public void setDetailsData(PoolDataDetails detailsData) {
        this.detailsData = detailsData;
    }

    public void setFlushData(FlushControllerData flushData) {
        this.flushData = flushData;
    }

    public void setHsmFlushQMData(
                    HSMFlushQManagerData hsmFlushQMData) {
        this.hsmFlushQMData = hsmFlushQMData;
    }

    public void setJtmData(JobTimeoutManagerData jtmData) {
        this.jtmData = jtmData;
    }

    public void setLinks(Set<String> links) {
        this.links = links;
    }

    public void setMigrationData(
                    MigrationData migrationData) {
        this.migrationData = migrationData;
    }

    public void setPoolGroups(List<String> poolGroups) {
        this.poolGroups = poolGroups;
    }

    public void setPpData(P2PData ppData) {
        this.ppData = ppData;
    }

    public void setRepositoryData(
                    RepositoryData repositoryData) {
        this.repositoryData = repositoryData;
    }

    public void setStorageHandlerData(
                    StorageHandlerData storageHandlerData) {
        this.storageHandlerData = storageHandlerData;
    }

    public void setSweeperData(SweeperData sweeperData) {
        this.sweeperData = sweeperData;
    }

    public void setTransferServicesData(
                    TransferServicesData transferServicesData) {
        this.transferServicesData = transferServicesData;
    }
}
