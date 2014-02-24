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
package org.dcache.chimera;

import javax.sql.DataSource;

import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.PoolManagerGetPoolMonitor;
import diskCacheV111.vehicles.ProtocolInfo;

import org.dcache.acl.enums.AccessMask;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.pinmanager.PinManagerUnpinMessage;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.vehicles.FileAttributes;

/**
 * Overrides protected methods so as to be able to provide live locality
 * information if requested; the latter calls the PNFS and Pool managers.
 * Also implements requests to pin manager for STAGE command.
 *
 * @author arossi
 */
public class DCacheAwareJdbcFs extends JdbcFs {
    private CellStub poolManagerStub;
    private CellStub pinManagerStub;
    private PnfsHandler pnfsHandler;

    public DCacheAwareJdbcFs(DataSource dataSource, String dialect) {
        super(dataSource, dialect);
    }

    public DCacheAwareJdbcFs(DataSource dataSource, String dialect, int id) {
        super(dataSource, dialect, id);
    }

    public void setPnfsHandler(PnfsHandler pnfsHandler) {
        this.pnfsHandler = pnfsHandler;
    }

    public void setPoolManagerStub(CellStub poolManagerStub) {
        this.poolManagerStub = poolManagerStub;
    }

    public void setPinManagerStub(CellStub pinManagerStub) {
        this.pinManagerStub = pinManagerStub;
    }

    @Override
    public String getFileLocality(FsInode_PLOC node) throws ChimeraFsException {
        FsInode pathInode = new FsInode(DCacheAwareJdbcFs.this, node.toString());
        return getFileLocality(inode2path(pathInode));
    }

    /**
     * This method sends a request to the pin manager to pin
     * a given file.
     */
    @Override
    public void pin(String pnfsid, long lifetime) throws ChimeraFsException {
        FileAttributes attributes = new FileAttributes();
        attributes.setPnfsId(new PnfsId(pnfsid));
        /*
         * TODO improve code to pass in the actual InetAddress of the
         * client so that link net masks do not interfere; note that SRM uses
         * "localhost", so it is not a deviation from existing behavior.
         */
        ProtocolInfo protocolInfo
            =  new DCapProtocolInfo("DCap", 3, 0, new InetSocketAddress("localhost", 0));
        PinManagerPinMessage message
            = new PinManagerPinMessage(attributes, protocolInfo, null, lifetime);

        try {
            pinManagerStub.sendAndWait(message);
        } catch (CacheException | InterruptedException t) {
            throw new ChimeraFsException("pin", t);
        }
    }

    /**
     * This method sends a request to the pin manager to unpin
     * a given file.
     */
    @Override
    public void unpin(String pnfsid) throws ChimeraFsException {
        PinManagerUnpinMessage message
            = new PinManagerUnpinMessage(new PnfsId(pnfsid));

        try {
            pinManagerStub.sendAndWait(message);
        } catch (CacheException | InterruptedException t) {
            throw new ChimeraFsException("unpin", t);
        }
    }

    /**
     * Callout to get pool monitor and check for live (network) status of a file
     * instead of simply its status as recorded in the Chimera database.
     */
    private String getFileLocality(String filePath) throws ChimeraFsException {
        PoolMonitor _poolMonitor;
        FileLocality locality = FileLocality.UNAVAILABLE;

        try {
            _poolMonitor = poolManagerStub.sendAndWait(
                            new PoolManagerGetPoolMonitor()).getPoolMonitor();

            Set<FileAttribute> requestedAttributes
                = EnumSet.of(FileAttribute.TYPE,
                             FileAttribute.SIZE,
                             FileAttribute.STORAGEINFO,
                             FileAttribute.LOCATIONS);
            Set<AccessMask> accessMask = EnumSet.of(AccessMask.READ_DATA);
            FileAttributes attributes
                = pnfsHandler.getFileAttributes(filePath, requestedAttributes,
                                                accessMask);
            /*
             * TODO improve code to pass in the actual InetAddress of the
             * client so that link net masks do not interfere; note that SRM uses
             * "localhost", so it is not a deviation from existing behavior.
             */
            locality = _poolMonitor.getFileLocality(attributes, "localhost");
        } catch (CacheException | InterruptedException t) {
            throw new ChimeraFsException("getFileLocality", t);
        }

        return locality.toString();
    }
}
