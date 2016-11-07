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

import com.google.common.collect.ImmutableList;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnitV2;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.pools.PoolV2Mode;
import dmg.cells.nucleus.CellAddressCore;
import org.dcache.vehicles.FileAttributes;

final class TestSelectionUnit implements PoolSelectionUnit {
    PoolSelectionUnitV2 psu;

    @Override
    public String[] getActivePools() {
        return psu.getActivePools();
    }

    @Override
    public Collection<SelectionPool> getAllDefinedPools(boolean enabledOnly) {
        return psu.getAllDefinedPools(enabledOnly);
    }

    @Override
    public String[] getDefinedPools(boolean enabledOnly) {
        return psu.getDefinedPools(enabledOnly);
    }

    @Override
    public SelectionLink getLinkByName(String linkName)
                    throws NoSuchElementException {
        return psu.getLinkByName(linkName);
    }

    @Override
    public SelectionLinkGroup getLinkGroupByName(String linkGroupName)
                    throws NoSuchElementException {
        return psu.getLinkGroupByName(linkGroupName);
    }

    @Override
    public Map<String, SelectionLinkGroup> getLinkGroups() {
        return psu.getLinkGroups();
    }

    @Override
    public Map<String, SelectionLink> getLinks() {
        return psu.getLinks();
    }

    @Override
    public Collection<SelectionLink> getLinksPointingToPoolGroup(
                    String poolGroup) throws NoSuchElementException {
        return psu.getLinksPointingToPoolGroup(poolGroup);
    }

    @Override
    public String getNetIdentifier(String address) throws UnknownHostException {
        return psu.getNetIdentifier(address);
    }

    @Override
    public SelectionPool getPool(String poolName) {
        return psu.getPool(poolName);
    }

    @Override
    public boolean isEnabledRegex() {
        return psu.isEnabledRegex();
    }

    @Override
    public boolean updatePool(String poolName, CellAddressCore address,
                              long serialId, PoolV2Mode mode,
                              Set<String> hsmInstances) {
        return psu.updatePool(poolName, address, serialId, mode, hsmInstances);
    }

    @Override
    public Map<String, SelectionPoolGroup> getPoolGroups() {
        return psu.getPoolGroups();
    }

    @Override
    public Collection<SelectionPoolGroup> getPoolGroupsOfPool(String poolName) {
        return psu.getPoolGroupsOfPool(poolName);
    }

    @Override
    public Map<String, SelectionPool> getPools() {
        return psu.getPools();
    }

    @Override
    public Collection<SelectionPool> getPoolsByPoolGroup(String poolGroup)
                    throws NoSuchElementException {
        return psu.getPoolsByPoolGroup(poolGroup);
    }

    @Override
    public String getProtocolUnit(String protocolUnitName) {
        return psu.getProtocolUnit(protocolUnitName);
    }

    @Override
    public Map<String, SelectionUnit> getSelectionUnits() {
        return psu.getSelectionUnits();
    }

    @Override
    public StorageUnit getStorageUnit(String storageClass) {
        return psu.getStorageUnit(storageClass);
    }

    @Override
    public Map<String, SelectionUnitGroup> getUnitGroups() {
        return psu.getUnitGroups();
    }

    @Override
    public String getVersion() {
        return psu.getVersion();
    }

    @Override
    public PoolPreferenceLevel[] match(DirectionType type, String net,
                    String protocol, FileAttributes fileAttributes,
                    String linkGroup) {
        return psu.match(type, net, protocol, fileAttributes, linkGroup);
    }

    String getInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append("UNITS:\n");
        psu.getSelectionUnits().values().stream().forEach(
                        (u) -> builder.append("\t").append(u).append("\n"));
        builder.append("UNIT GROUPS:\n");
        psu.getUnitGroups().values().stream().forEach(
                        (u) -> builder.append("\t").append(u).append("\n"));
        builder.append("LINKS:\n");
        psu.getLinks().values().stream().forEach(
                        (u) -> builder.append("\t").append(u).append("\n"));
        builder.append("POOL GROUPS:\n");
        psu.getPoolGroups().values().stream().forEach(
                        (u) -> builder.append("\t").append(u.getName()).append(
                                        "\t").append(u).append("\n"));
        builder.append("POOLS:\n");
        psu.getPools().values().stream().forEach(
                        (u) -> builder.append("\t").append(u).append("\n"));
        return builder.toString();
    }

    void load() {
        PoolSelectionUnitV2 v2 = new PoolSelectionUnitV2();
        createUnits(v2);
        createUnitGroups(v2);
        createPools(v2);
        createPoolGroups(v2);
        createLinks(v2);
        psu = v2;
    }

    void makeStorageUnitNonResilient(String unitKey) {
        psu.setStorageUnit(unitKey, 1, new String[0]);
    }

    void setOffline(String... offline) {
        for (String pool : offline) {
            psu.setActive(pool, false);
            psu.setPoolDisabled(pool);
        }
    }

    void setUseRegex() {
        psu.setRegex("on");
    }

    private void createLinks(PoolSelectionUnitV2 psu) {
        for (int i = 0; i < TestData.LINKS.length; ++i) {
            String link = TestData.LINKS[i][0];
            psu.createLink(link, ImmutableList.of(TestData.LINKS[i][1],
                            TestData.LINKS[i][2], TestData.LINKS[i][3]));
            psu.setLink(link, TestData.LINKS_SET[i][0],
                            TestData.LINKS_SET[i][1], TestData.LINKS_SET[i][2],
                            TestData.LINKS_SET[i][3], TestData.LINKS_SET[i][4]);
            for (String toAdd : TestData.LINKS_ADD[i]) {
                psu.addLink(link, toAdd);
            }
        }
    }

    private void createPoolGroups(PoolSelectionUnitV2 psu) {
        for (int i = 0; i < TestData.POOL_GROUPS.length; ++i) {
            String name = TestData.POOL_GROUPS[i][0];
            psu.createPoolGroup(name, "-resilient".equals(TestData.POOL_GROUPS[i][1]));
            String prefix = name.substring(0, name.indexOf("-"));
            psu.getAllDefinedPools(false).stream().filter(
                            (p) -> p.getName().startsWith(prefix)).forEach(
                            (p) -> psu.addToPoolGroup(name, p.getName()));
        }
    }

    private void createPools(PoolSelectionUnitV2 psu) {
        for (int i = 0; i < TestData.POOL_TYPE.length; ++i) {
            String prefix = TestData.POOL_TYPE[i];
            for (int k = 0; k < TestData.POOL_COUNT[i]; ++k) {
                String pool = prefix + k;
                psu.createPool(pool, false, false, false);
                psu.setActive(pool, true);
                psu.setPoolEnabled(pool);
                psu.getPool(pool).setPoolMode(
                                new PoolV2Mode(PoolV2Mode.ENABLED));
            }
        }
    }

    private void createUnitGroups(PoolSelectionUnitV2 psu) {
        for (String unitGroup : TestData.UNIT_GROUPS) {
            psu.createUnitGroup(unitGroup);
        }

        for (int i = 0; i < TestData.UNIT_GROUPS_ADD.length; ++i) {
            psu.addToUnitGroup(TestData.UNIT_GROUPS_ADD[i][0],
                            TestData.UNIT_GROUPS_ADD[i][1],
                            Boolean.parseBoolean(
                                            TestData.UNIT_GROUPS_ADD[i][2]));
        }
    }

    private void createUnits(PoolSelectionUnitV2 psu) {
        for (String unit : TestData.PROTOCOL_UNITS) {
            psu.createUnit(unit, false, false, false, true);
        }

        for (String unit : TestData.NET_UNITS) {
            psu.createUnit(unit, true, false, false, false);
        }

        for (int i = 0; i < TestData.STORAGE_UNITS.length; ++i) {
            psu.createUnit(TestData.STORAGE_UNITS[i], false, true, false,
                            false);
            if (TestData.STORAGE_UNITS_SET[i] != null) {
                psu.setStorageUnit(TestData.STORAGE_UNITS[i],
                                Integer.parseInt(TestData.STORAGE_UNITS_SET[i][0]),
                                TestData.STORAGE_UNITS_SET[i][1].split("[,]"));
            }
        }
    }
}
