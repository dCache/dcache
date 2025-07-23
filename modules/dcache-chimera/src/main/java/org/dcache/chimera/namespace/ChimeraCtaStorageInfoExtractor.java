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
package org.dcache.chimera.namespace;

import com.google.common.collect.ImmutableList;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.CtaStorageInfo;
import diskCacheV111.vehicles.StorageInfo;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileState;
import org.dcache.chimera.StorageGenericLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ChimeraCtaStorageInfoExtractor extends ChimeraHsmStorageInfoExtractor {


    private static final Logger LOGGER = LoggerFactory.getLogger(
          ChimeraHsmStorageInfoExtractor.class);


    public ChimeraCtaStorageInfoExtractor(AccessLatency defaultAL,
                                          RetentionPolicy defaultRP) {
        super(defaultAL, defaultRP);
    }


    @Override
    public StorageInfo getFileStorageInfo(ExtendedInode inode) throws CacheException {
        try {

            CtaStorageInfo parentStorageInfo = (CtaStorageInfo) getDirStorageInfo(inode);

            List<String> locations = inode.
                getLocations(StorageGenericLocation.TAPE);

            if (locations.isEmpty()) {
                if (inode.statCache().getState() != FileState.CREATED) {
                    parentStorageInfo.setIsNew(false);
                }
                return parentStorageInfo;
            } else {
                StorageInfo info = new CtaStorageInfo(parentStorageInfo.getStorageGroup(),
                                                      parentStorageInfo.getFileFamily());
                info.setIsNew(false);
                for (String location : locations) {
                    try {
                        info.addLocation(new URI(location));
                    } catch (URISyntaxException e) {
                        LOGGER.debug("Ignoring bad tape location {}: {}",
                                     location, e.toString());
                    }
                }
                return info;
            }
        } catch (ChimeraFsException e) {
            throw new CacheException(e.getMessage());
        }
    }

    @Override
    public StorageInfo getDirStorageInfo(ExtendedInode inode) throws CacheException {
        ExtendedInode directory = inode.isDirectory() ?
            inode : inode.getParent();

        if (directory == null) {
            throw new FileNotFoundCacheException("file unlinked");
        }

        ImmutableList<String> group = directory.getTag("storage_group");
        ImmutableList<String> family = directory.getTag("file_family");

        CtaStorageInfo info;

        if (!group.isEmpty()) {
            /**
             * Enstore
             */
            String sg = getFirstLine(group).map(String::intern).orElse("none");
            String ff = getFirstLine(family).map(String::intern).orElse("none");
            info = new CtaStorageInfo(sg, ff);
        } else {
            /**
             * OSM
             */
            List<String> osmTemplateTag = directory.getTag("OSMTemplate");

            Map<String, String> hash = new HashMap<>();
            osmTemplateTag.stream()
                .map(StringTokenizer::new)
                .filter(t -> t.countTokens() >= 2)
                .forEach(t -> hash.put(t.nextToken().intern(), t.nextToken()));

            String storeName = hash.get("StoreName");

            if (storeName == null && !osmTemplateTag.isEmpty()) {
                LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.STORAGE_CLASS_MISCONFIGURED),
                        "StoreName not found in template. Directory: {}", pathOrId(directory));
                throw new CacheException(CacheException.STORAGE_CLASS_MISCONFIGURED, "StoreName not found in template");
            }

            List<String> sGroupTag = directory.getTag("sGroup");
            String sGroup = getFirstLine(sGroupTag).map(String::intern).orElse(null);
            info = new CtaStorageInfo(storeName, sGroup);
        }

        return info;
    }

    private static String pathOrId(ExtendedInode inode) {
        try {
            return inode.getPath().toString();
        } catch (ChimeraFsException e) {
            try {
                return inode.getId();
            } catch (ChimeraFsException ex) {
                // should never happen as we already read the inode
                throw new RuntimeException(ex);
            }
        }
    }
}
