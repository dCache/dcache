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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import javax.sql.DataSource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.PoolManagerGetPoolMonitor;

import org.dcache.acl.enums.AccessMask;
import org.dcache.alarms.IAlarms;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.vehicles.FileAttributes;

/**
 * Overrides protected methods so as to be able to provide live locality
 * information if requested. Embeds a Guava cache to maintain already
 * initialized PGET nodes; the latter is calls out in turn to the PNFS and Pool
 * managers.
 *
 * @author arossi
 */
public class DCacheAwareJdbcFs extends JdbcFs {

    private final class LocalityArgs {
        private final FsInode parent;
        private final String id;
        private final String[] args;

        private LocalityArgs(FsInode parent, String[] args) {
            this.parent = parent;
            this.id = parent.toString();
            this.args = args.clone();
        }

        private LocalityArgs(JdbcFs fs, String id, String[] args) {
            this.parent = new FsInode(fs, id);
            this.id = id;
            this.args = args.clone();
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof LocalityArgs)) {
                return false;
            }

            LocalityArgs largs = (LocalityArgs) o;

            if (!id.equals(largs.id)) {
                return false;
            }

            return Arrays.equals(args, largs.args);
        }

        public int hashCode() {
            return toString().hashCode();
        }

        public String toString() {
            return id + Arrays.asList(args);
        }

    }

    private final class LocalityLoader extends
                    CacheLoader<LocalityArgs, FsInode_PGET> {

        @Override
        public FsInode_PGET load(LocalityArgs key) throws Exception {
            FsInode_PGET pget = getSuperPGET(key.parent, key.args);
            if (pget.hasMetadataFor(LOCALITY)) {
                FsInode pathInode = inodeOf(key.parent, pget.getName());
                String path = inode2path(pathInode);
                String locality = getFileLocality(path);
                pget.setMetadata(LOCALITY, locality);
                /*
                 * need to override the cached stat value
                 */
                pget.stat();
            }
            return pget;
        }
    }

    private static final String LOCALITY = "locality";

    private static String host;

    static {
        try {
            host = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            host = IAlarms.UNKNOWN_HOST;
        }
    }

    private CellStub poolManagerStub;
    private PnfsHandler pnfsHandler;

    /**
     * Short-term caching of locality information. Policy uses expire after
     * write because we do not want to get too far out of synchronization with
     * the live system. Maintains entries for 1 minute.
     */
    private final LoadingCache<LocalityArgs, FsInode_PGET> CACHE
        = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES)
                                   .maximumSize(64)
                                   .softValues().build(new LocalityLoader());

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

    /**
     * Goes to the cache instead of directly creating the inode.
     */
    protected FsInode_PGET getPGET(FsInode parent, String[] cmd)
                    throws ChimeraFsException {
        try {
            return CACHE.get(new LocalityArgs(parent, cmd));
        } catch (ExecutionException t) {
            Throwable cause = t.getCause();
            if (cause instanceof ChimeraFsException) {
                throw (ChimeraFsException)cause;
            }
            throw new ChimeraFsException(t.toString());
        }
    }

    /**
     * Goes to the cache instead of directly creating the inode.
     */
    protected FsInode_PGET getPGET(String id, String[] cmd)
                    throws ChimeraFsException {
        try {
            return CACHE.get(new LocalityArgs(this, id, cmd));
        } catch (ExecutionException t) {
            Throwable cause = t.getCause();
            if (cause instanceof ChimeraFsException) {
                throw (ChimeraFsException)cause;
            }
            throw new ChimeraFsException(t.toString());
        }
    }

    /**
     * Callout to get pool monitor and check for live (network) status of a file
     * instead of simply its status as recorded in the Chimera database.
     */
    private String getFileLocality(String filePath) throws IOException {
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
            locality = _poolMonitor.getFileLocality(attributes, host);
        } catch (CacheException | InterruptedException t) {
            throw new IOException(t);
        }

        return locality.toString();
    }

    private FsInode_PGET getSuperPGET(FsInode parent, String[] cmd)
                    throws ChimeraFsException {
        return super.getPGET(parent, cmd);
    }
}
