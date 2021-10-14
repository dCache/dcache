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

import com.google.common.base.Throwables;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.NoRouteToCellException;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.AccessController;
import java.sql.SQLException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.curator.shaded.com.google.common.collect.ImmutableMap;
import org.dcache.acl.enums.AccessMask;
import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.pinmanager.PinManagerListPinsMessage;
import org.dcache.pinmanager.PinManagerListPinsMessage.Info;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.pinmanager.PinManagerUnpinMessage;
import org.dcache.poolmanager.RemotePoolMonitor;
import org.dcache.vehicles.FileAttributes;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.transaction.PlatformTransactionManager;


/**
 * Overrides protected methods so as to be able to provide live locality information if requested;
 * the latter calls the PNFS and Pool managers. Also implements requests to pin manager for STAGE
 * command.
 *
 * @author arossi
 */
public class DCacheAwareJdbcFs extends JdbcFs implements CellIdentityAware {

    public class DcachePinInfo implements PinInfo {

        private final Instant creation;
        private final Optional<Instant> expiration;
        private final PinState state;
        private final Optional<String> requestId;
        private final long id;
        private final boolean isUnpinnable;

        public DcachePinInfo(Info info) {
            creation = info.getCreationTime();
            expiration = info.getExpirationTime();
            state = TO_PIN_STATE.get(info.getState());
            requestId = info.getRequestId();
            id = info.getId();
            isUnpinnable = info.isUnpinnable();
        }

        @Override
        public Instant getCreationTime() {
            return creation;
        }

        @Override
        public Optional<Instant> getExpirationTime() {
            return expiration;
        }

        @Override
        public PinState getState() {
            return state;
        }

        @Override
        public Optional<String> getRequestId() {
            return requestId;
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public boolean isUnpinnable() {
            return isUnpinnable;
        }
    }

    private static final Map<PinManagerListPinsMessage.State, PinState> TO_PIN_STATE = ImmutableMap.of(
          PinManagerListPinsMessage.State.PINNING, PinState.PINNING,
          PinManagerListPinsMessage.State.PINNED, PinState.PINNED,
          PinManagerListPinsMessage.State.UNPINNING, PinState.UNPINNING);

    private CellStub pinManagerStub;
    private CellStub billingStub;
    private PnfsHandler pnfsHandler;
    private CellAddressCore myAddress;
    private boolean queryPnfsManagerOnRename;

    /**
     * Pool manager's runtime configuration.
     */
    private RemotePoolMonitor poolMonitor;

    @Required
    public void setQueryPnfsManagerOnRename(boolean yes) {
        queryPnfsManagerOnRename = yes;
    }

    public DCacheAwareJdbcFs(DataSource dataSource, PlatformTransactionManager txManager)
          throws ChimeraFsException, SQLException {
        super(dataSource, txManager);
    }

    public DCacheAwareJdbcFs(DataSource dataSource, PlatformTransactionManager txManager, int id)
          throws ChimeraFsException, SQLException {
        super(dataSource, txManager, id);
    }

    public void setPnfsHandler(PnfsHandler pnfsHandler) {
        this.pnfsHandler = pnfsHandler;
    }

    public void setPinManagerStub(CellStub pinManagerStub) {
        this.pinManagerStub = pinManagerStub;
    }

    public void setBillingStub(CellStub billingStub) {
        this.billingStub = billingStub;
    }

    public void setPoolMonitor(RemotePoolMonitor poolMonitor) {
        this.poolMonitor = poolMonitor;
    }

    @Override
    public void setCellAddress(CellAddressCore address) {
        myAddress = address;
    }

    @Override
    public String getFileLocality(FsInode_PLOC node) throws ChimeraFsException {
        FsInode pathInode = new FsInode(DCacheAwareJdbcFs.this, node.ino());
        return getFileLocality(inode2path(pathInode));
    }

    /**
     * This method sends a request to the pin manager to pin a given file.
     */
    @Override
    public void pin(FsInode inode, long lifetime) throws ChimeraFsException {
        Subject subject = Subject.getSubject(AccessController.getContext());
        InetAddress client = Subjects.getOrigin(subject).getAddress();
        ProtocolInfo protocolInfo
              = new DCapProtocolInfo("DCap", 3, 0, new InetSocketAddress(client, 0));
        PinManagerPinMessage message
              = new PinManagerPinMessage(FileAttributes.ofPnfsId(inode.getId()),
              protocolInfo, null, lifetime);
        message.setSubject(subject);

        message.setReplyWhenStarted(true);

        try {
            pinManagerStub.sendAndWait(message);
        } catch (NoRouteToCellException | InterruptedException | CacheException e) {
            /* We "notify" the client that there was a problem pinning the
             * the file by returning NFSERR_INVAL back to the client.  The Linux
             * kernel should convert this to an EINVAL response; e.g.,
             *
             *     paul@sprocket:/mnt/tape$ touch ".(fset)(test.dat)(pin)(60)"
             *     touch: setting times of '.(fset)(test.dat)(pin)(60)': Invalid argument
             */
            throw new InvalidArgumentChimeraException(e.getMessage());
        }
    }

    /**
     * This method sends a request to the pin manager to unpin a given file.
     */
    @Override
    public void unpin(FsInode inode) throws ChimeraFsException {
        PinManagerUnpinMessage message
              = new PinManagerUnpinMessage(new PnfsId(inode.getId()));
        Subject subject = Subject.getSubject(AccessController.getContext());
        message.setSubject(subject);

        try {
            pinManagerStub.sendAndWait(message);
        } catch (PermissionDeniedCacheException e) {
            /* Trigger returning NFSERR_PERM back to client.  The Linux kernel
             * should convert this to an EPERM response.
             */
            throw new PermissionDeniedChimeraFsException(e.getMessage(), e);
        } catch (NoRouteToCellException | InterruptedException | CacheException e) {
            /* We "notify" the client that there was a problem unpinning the
             * the file by returning NFSERR_INVAL back to the client.  The Linux
             * kernel should convert this to an EINVAL response.
             */
            throw new InvalidArgumentChimeraException(e.getMessage(), e);
        }
    }

    @Override
    public List<PinInfo> listPins(FsInode inode) throws ChimeraFsException {
        PnfsId pnfsid = new PnfsId(inode.getId());
        PinManagerListPinsMessage request = new PinManagerListPinsMessage(pnfsid);
        Subject subject = Subject.getSubject(AccessController.getContext());
        request.setSubject(subject);
        try {
            return pinManagerStub.sendAndWait(request).getInfo().stream()
                  .map(DcachePinInfo::new)
                  .collect(Collectors.toList());
        } catch (NoRouteToCellException | InterruptedException | CacheException e) {
            throw new InvalidArgumentChimeraException(e.getMessage());
        }
    }

    @Override
    public void remove(FsInode directory, String name, FsInode inode) throws ChimeraFsException {

        super.remove(directory, name, inode);
        Subject subject = Subject.getSubject(AccessController.getContext());
        DoorRequestInfoMessage infoRemove
              = new DoorRequestInfoMessage(myAddress, "remove");

        infoRemove.setSubject(subject);
        infoRemove.setPnfsId(new PnfsId(inode.getId()));
        infoRemove.setFileSize(0L);
        infoRemove.setBillingPath("parent:[" + directory.getId() + "]/" + name);
        // FIXME: in some cases subject is not set
        infoRemove.setClient(subject == null ? "0.0.0.0"
              : Subjects.getOrigin(subject).getAddress().getHostAddress());
        infoRemove.setClientChain(infoRemove.getClient());

        billingStub.notify(infoRemove);
    }

    /**
     * Callout to get pool monitor and check for live (network) status of a file instead of simply
     * its status as recorded in the Chimera database.
     */
    private String getFileLocality(String filePath) throws ChimeraFsException {
        FileLocality locality = FileLocality.UNAVAILABLE;

        try {
            Set<FileAttribute> requestedAttributes
                  = EnumSet.of(FileAttribute.TYPE,
                  FileAttribute.SIZE,
                  FileAttribute.STORAGEINFO,
                  FileAttribute.LOCATIONS);
            Set<AccessMask> accessMask = EnumSet.of(AccessMask.READ_DATA);
            FileAttributes attributes
                  = pnfsHandler.getFileAttributes(filePath, requestedAttributes,
                  accessMask, false);
            /*
             * TODO improve code to pass in the actual InetAddress of the
             * client so that link net masks do not interfere; note that SRM uses
             * "localhost", so it is not a deviation from existing behavior.
             */
            locality = poolMonitor.getFileLocality(attributes, "localhost");
        } catch (CacheException t) {
            throw new ChimeraFsException("getFileLocality", t);
        }

        return locality.toString();
    }

    @Override
    public boolean rename(FsInode inode, FsInode srcDir, String source, FsInode destDir,
          String dest) throws ChimeraFsException {
        if (!queryPnfsManagerOnRename) {
            return super.rename(inode, srcDir, source, destDir, dest);
        }
        boolean rc = true;
        try {
            String sourceDirectory = inode2path(srcDir);
            File sourcePath = new File(sourceDirectory, source);
            String destinationDirectory = inode2path(destDir);
            File destinationPath = new File(destinationDirectory, dest);
            pnfsHandler.renameEntry(sourcePath.getCanonicalPath(),
                  destinationPath.getCanonicalPath(), true);
        } catch (PermissionDeniedCacheException e) {
            throw new PermissionDeniedChimeraFsException(e.getMessage());
        } catch (CacheException | IOException e) {
            Throwables.propagateIfInstanceOf(e, ChimeraFsException.class);
            throw new ChimeraFsException(e.getMessage(), e);
        }
        return rc;
    }
}
