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
package org.dcache.resilience;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import org.dcache.resilience.data.FileUpdate;
import org.dcache.resilience.db.LocalNamespaceAccess;
import org.dcache.resilience.db.ScanSummary;
import org.dcache.vehicles.FileAttributes;

public final class TestNamespaceAccess extends LocalNamespaceAccess {
    final Multimap<String, FileAttributes> locationsToFiles = ArrayListMultimap.create();
    final Map<PnfsId, FileAttributes>      fileAttributes   = new HashMap<>();

    public String getInfo() {
        StringBuilder builder = new StringBuilder();
        String[] locations = locationsToFiles.keySet().toArray(new String[0]);
        Arrays.sort(locations);

        for (String location : locations) {
            builder.append(location).append("\n");
            locationsToFiles.get(location).stream().forEach((fa) -> {
                builder.append("\tPnfsId:           ").append(
                                fa.getPnfsId()).append("\n");
                builder.append("\tAccessLatency:    ").append(
                                fa.getAccessLatency()).append("\n");
                builder.append("\tRetentionPolicy:  ").append(
                                fa.getRetentionPolicy()).append("\n");
                builder.append("\tStorageClass:     ").append(
                                fa.getStorageClass()).append("\n");
                builder.append("\tHsm:              ").append(
                                fa.getHsm()).append("\n");
                builder.append("\tLocations:\n");
                fa.getLocations().stream().forEach(
                                (l) -> builder.append("\t\t").append(l).append(
                                                "\n"));
            });
        }

        return builder.toString();
    }

    @Override
    public FileAttributes getRequiredAttributes(PnfsId pnfsId)
                    throws CacheException {
        if (fileAttributes.containsKey(pnfsId)) {
            return fileAttributes.get(pnfsId);
        }
        throw new FileNotFoundCacheException(pnfsId.toString());
    }

    @Override
    public void handlePnfsidsForPool(ScanSummary scan)
                    throws CacheException {
        for (FileAttributes attributes : locationsToFiles.get(scan.getPool())) {
            FileUpdate data = new FileUpdate(attributes.getPnfsId(),
                                             scan.getPool(),
                                             scan.getType(),
                                             scan.getGroup(),
                                             scan.isForced());
            if (handler.handleScannedLocation(data, scan.getStorageUnit())) {
                scan.incrementCount();
            }
        }
    }

    @Override
    public void refreshAttributes(FileAttributes attributes)
                    throws CacheException {
        FileAttributes stored = fileAttributes.get(attributes.getPnfsId());
        if (stored != null) {
            attributes.setAccessLatency(stored.getAccessLatency());
            attributes.setAccessTime(stored.getAccessTime());
            attributes.setHsm(stored.getHsm());
            attributes.setRetentionPolicy(stored.getRetentionPolicy());
            attributes.setStorageClass(stored.getStorageClass());
            attributes.setLocations(stored.getLocations());
        } else {
            throw new FileNotFoundCacheException(attributes.getPnfsId().toString(),
                                                 new Exception("test simulation"));
        }
    }

    void clear() {
        fileAttributes.clear();
        locationsToFiles.clear();
    }

    void delete(PnfsId pnfsId, boolean locationsOnly) {
        FileAttributes attributes = locationsOnly ?
                        fileAttributes.get(pnfsId) :
                        fileAttributes.remove(pnfsId);
        attributes.getLocations().stream().forEach(
                        (l) -> locationsToFiles.remove(l, attributes));
        attributes.getLocations().clear();
    }

    void loadExcessResilient() {
        for (int i = 0; i < TestData.REPLICA_ONLINE.length; ++i) {
            loadRequired(TestData.REPLICA_ONLINE[i], AccessLatency.ONLINE,
                         RetentionPolicy.REPLICA, TestData.HSM,
                         TestData.STORAGE_CLASSES[i],
                         TestData.EXCESS_RESILIENT_LOCATIONS[i]);
        }

        for (int i = 0; i < TestData.CUSTODIAL_ONLINE.length; ++i) {
            loadRequired(TestData.CUSTODIAL_ONLINE[i], AccessLatency.ONLINE,
                         RetentionPolicy.CUSTODIAL, TestData.HSM,
                         TestData.STORAGE_CLASSES[i],
                         TestData.EXCESS_RESILIENT_LOCATIONS[i]);
        }
    }

    void loadNewResilient() {
        for (int i = 0; i < TestData.REPLICA_ONLINE.length; ++i) {
            loadRequired(TestData.REPLICA_ONLINE[i], AccessLatency.ONLINE,
                         RetentionPolicy.REPLICA, TestData.HSM,
                         TestData.STORAGE_CLASSES[i],
                         TestData.NEW_RESILIENT_LOCATIONS[i]);
        }

        for (int i = 0; i < TestData.CUSTODIAL_ONLINE.length; ++i) {
            loadRequired(TestData.CUSTODIAL_ONLINE[i], AccessLatency.ONLINE,
                         RetentionPolicy.CUSTODIAL, TestData.HSM,
                         TestData.STORAGE_CLASSES[i],
                         TestData.NEW_RESILIENT_LOCATIONS[i]);
        }
    }

    void loadNewResilientOnHostAndRackTagsDefined() {
        for (int i = 0; i < TestData.REPLICA_ONLINE.length; ++i) {
            loadRequired(TestData.REPLICA_ONLINE[i], AccessLatency.ONLINE,
                         RetentionPolicy.REPLICA, TestData.HSM,
                         TestData.STORAGE_CLASSES[i],
                         TestData.NEW_RESILIENT_LOCATIONS_HR[i]);
        }

        for (int i = 0; i < TestData.CUSTODIAL_ONLINE.length; ++i) {
            loadRequired(TestData.CUSTODIAL_ONLINE[i], AccessLatency.ONLINE,
                         RetentionPolicy.CUSTODIAL, TestData.HSM,
                         TestData.STORAGE_CLASSES[i],
                         TestData.NEW_RESILIENT_LOCATIONS_HR[i]);
        }
    }

    void loadNewResilientWithUnmappedStorageUnit() {
        for (int i = 0; i < TestData.REPLICA_ONLINE.length; ++i) {
            loadRequired(TestData.REPLICA_ONLINE[i], AccessLatency.ONLINE,
                         RetentionPolicy.REPLICA, TestData.HSM,
                         "unmapped-storage",
                         TestData.NEW_RESILIENT_LOCATIONS_H[i]);
        }
    }

    void loadNewFilesWithStorageUnitMatchingPattern(){
        for (int i = 0; i < TestData.REPLICA_ONLINE.length; ++i) {
            loadRequired(TestData.REPLICA_ONLINE[i], AccessLatency.ONLINE,
                         RetentionPolicy.REPLICA, TestData.HSM,
                         "test-storage",
                         TestData.NEW_RESILIENT_LOCATIONS_H[i]);
        }
    }

    void loadNewResilientOnHostTagDefined() {
        for (int i = 0; i < TestData.REPLICA_ONLINE.length; ++i) {
            loadRequired(TestData.REPLICA_ONLINE[i], AccessLatency.ONLINE,
                         RetentionPolicy.REPLICA, TestData.HSM,
                         TestData.STORAGE_CLASSES[i],
                         TestData.NEW_RESILIENT_LOCATIONS_H[i]);
        }

        for (int i = 0; i < TestData.CUSTODIAL_ONLINE.length; ++i) {
            loadRequired(TestData.CUSTODIAL_ONLINE[i], AccessLatency.ONLINE,
                         RetentionPolicy.CUSTODIAL, TestData.HSM,
                         TestData.STORAGE_CLASSES[i],
                         TestData.NEW_RESILIENT_LOCATIONS_H[i]);
        }
    }

    void loadNonResilient() {
        int k = TestData.REPLICA_ONLINE.length;
        for (int i = 0; i < TestData.CUSTODIAL_NEARLINE.length; ++i) {
            loadRequired(TestData.CUSTODIAL_NEARLINE[i], AccessLatency.NEARLINE,
                         RetentionPolicy.CUSTODIAL, TestData.HSM,
                         TestData.STORAGE_CLASSES[k++],
                         TestData.NON_RESILIENT_LOCATIONS[i]);
        }
    }

    void loadNonTaggedExcessResilient() {
        for (int i = 0; i < TestData.REPLICA_ONLINE.length; ++i) {
            loadRequired(TestData.REPLICA_ONLINE[i], AccessLatency.ONLINE,
                         RetentionPolicy.REPLICA, TestData.HSM,
                         TestData.STORAGE_CLASSES[i],
                         TestData.NON_TAGGED_EXCESS_RESILIENT_LOCATIONS[i]);
        }
    }

    void loadMissingResilientLocations() {
        for (int i = 0; i < TestData.REPLICA_ONLINE.length; ++i) {
            loadRequired(TestData.REPLICA_ONLINE[i], AccessLatency.ONLINE,
                         RetentionPolicy.REPLICA, TestData.HSM,
                         TestData.STORAGE_CLASSES[i],
                         TestData.MISSING_RESILIENT_LOCATIONS[i]);
        }

        for (int i = 0; i < TestData.CUSTODIAL_ONLINE.length; ++i) {
            loadRequired(TestData.CUSTODIAL_ONLINE[i], AccessLatency.ONLINE,
                         RetentionPolicy.CUSTODIAL, TestData.HSM,
                         TestData.STORAGE_CLASSES[i],
                         TestData.MISSING_RESILIENT_LOCATIONS[i]);
        }
    }

    void loadRequired(PnfsId pnfsId, AccessLatency accessLatency,
                      RetentionPolicy retentionPolicy, String hsm,
                      String storageClass, String... locations) {
        FileAttributes attr = new FileAttributes();
        attr.setAccessTime(System.currentTimeMillis());
        attr.setPnfsId(pnfsId);
        attr.setAccessLatency(accessLatency);
        attr.setRetentionPolicy(retentionPolicy);
        attr.setHsm(hsm);
        attr.setStorageClass(storageClass);
        attr.setLocations(Lists.newArrayList(locations));
        attr.setSize(2L);
        fileAttributes.put(pnfsId, attr);
        for (String pool : locations) {
            locationsToFiles.put(pool, attr);
        }
    }

    void loadRequiredResilient() {
        for (int i = 0; i < TestData.REPLICA_ONLINE.length; ++i) {
            loadRequired(TestData.REPLICA_ONLINE[i], AccessLatency.ONLINE,
                         RetentionPolicy.REPLICA, TestData.HSM,
                         TestData.STORAGE_CLASSES[i],
                         TestData.MIN_RESILIENT_LOCATIONS[i]);
        }

        for (int i = 0; i < TestData.CUSTODIAL_ONLINE.length; ++i) {
            loadRequired(TestData.CUSTODIAL_ONLINE[i], AccessLatency.ONLINE,
                         RetentionPolicy.CUSTODIAL, TestData.HSM,
                         TestData.STORAGE_CLASSES[i],
                         TestData.MIN_RESILIENT_LOCATIONS[i]);
        }
    }
}
