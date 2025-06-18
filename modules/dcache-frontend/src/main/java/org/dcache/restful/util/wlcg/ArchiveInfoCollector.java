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
package org.dcache.restful.util.wlcg;

import static com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly;
import static org.dcache.namespace.FileType.REGULAR;

import com.google.common.base.Throwables;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.dcache.acl.enums.AccessMask;
import org.dcache.namespace.FileAttribute;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.providers.tape.ArchiveInfo;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

public class ArchiveInfoCollector implements CellCommandListener {

    private static final Set<FileAttribute> REQUIRED_ATTRIBUTES
          = EnumSet.of(FileAttribute.TYPE, FileAttribute.SIZE, FileAttribute.STORAGEINFO,
          FileAttribute.LOCATIONS);
    private static final Set<AccessMask> ACCESS_MASK = EnumSet.of(AccessMask.READ_DATA);
    private static final int MAX_PATHS_DEFAULT = 10_000;

    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveInfoCollector.class);

    @Command(name = "archiveinfo maxpaths",
          hint = "set maximum number of file paths allowed in archive info request.",
          description = "If the request exceeds this limit, no error is generated, but "
                + "only the first N = maxpaths are processed.")
    class ArchiveInfoMaxPaths implements Callable<String> {
        @Argument(usage = "max number of paths.")
        int max = MAX_PATHS_DEFAULT;

        @Override
        public String call() throws Exception {
            maxPaths = max;
            return "Max paths now = " + maxPaths;
        }
    }

    private PoolMonitor poolMonitor;
    private ExecutorService service;
    private int maxPaths;

    public List<ArchiveInfo> getInfo(PnfsHandler pnfsHandler, String prefix, List<String> paths) {
        Map<String, Future<FileLocality>> futures = new HashMap<>();
        List<ArchiveInfo> infoList = new ArrayList<>();

        for (String path : paths) {
            futures.put(path, service.submit(() -> getInfo(path, prefix, pnfsHandler)));
        }

        for (Entry<String, Future<FileLocality>> future : futures.entrySet()) {
            ArchiveInfo info = new ArchiveInfo();
            info.setPath(future.getKey());

            try {
                info.setLocality(getUninterruptibly(future.getValue()));
            } catch (ExecutionException e) {
                LOGGER.error("getInfo failed for {}: {}.", future.getKey(),
                      Throwables.getRootCause(e).getMessage());
                info.setError(Throwables.getRootCause(e).getMessage());
            }

            infoList.add(info);
        }

        return infoList;
    }

    public int getMaxPaths() {
        return maxPaths;
    }

    @Required
    public void setMaxPaths(int maxPaths) {
        this.maxPaths = maxPaths;
    }

    @Required
    public void setPoolMonitor(PoolMonitor poolMonitor) {
        this.poolMonitor = poolMonitor;
    }

    @Required
    public void setService(ExecutorService service) {
        this.service = service;
    }

    private FileLocality getInfo(String path, String prefix, PnfsHandler pnfsHandler) throws CacheException {
        String absolutePath = computeFsPath(prefix, path).toString();
        FileAttributes attributes = pnfsHandler.getFileAttributes(absolutePath, REQUIRED_ATTRIBUTES,
              ACCESS_MASK, false);
        FileLocality locality = poolMonitor.getFileLocality(attributes, "localhost");
        /**
         * This is done to placate CERN FTS that refuses
         * to remove incomplete files if their locality is not ONLINE
         */
        if (attributes.getFileType() == REGULAR && locality == FileLocality.NONE) {
            if (!attributes.isDefined(FileAttribute.SIZE) || attributes.getSize() == 0) {
                locality = FileLocality.ONLINE;
            }
        }
        return locality;
    }

    public static FsPath computeFsPath(String prefix, String target) {
        FsPath absolutePath = FsPath.create(FsPath.ROOT + target);
        if (prefix != null) {
            FsPath pref = FsPath.create(prefix);
            if (!absolutePath.hasPrefix(pref)) {
                absolutePath = FsPath.create(
                                             FsPath.ROOT + (prefix.endsWith("/") ? prefix : prefix + "/") + target);
            }
        }
        return absolutePath;
    }

}
