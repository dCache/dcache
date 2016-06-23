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
package org.dcache.resilience.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.net.UnknownHostException;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnitAccess;
import diskCacheV111.poolManager.StorageUnit;

/**
 * <p>The purpose of this class is to enable the interception of changes
 *      from the command line interface and to react to them appropriately
 *      in terms of the resilience system.</p>
 *
 * <p>For some commands, validity checks are run.  In particular,
 *      the replication constraints as established by storage units
 *      are considered and links to resilient groups are followed
 *      to make sure constraints can be satisified, and that
 *      resilient storage groups are not linked to non-resilient pool groups.</p>
 *
 * <p>Note that the interface extends {@link dmg.cells.nucleus.CellSetupProvider}.
 *      This is in order to be able to deactivate the pre-processing procedures
 *      when the poolmanager.conf file is loaded.</p>
 *
 * <p>Changes will be handled on the resilience end via the reception of a
 *      {@link org.dcache.poolmanager.SerializablePoolMonitor} from
 *      the PoolManager.</p>
 */
public final class PoolSelectionUnitDecorator
                implements PoolSelectionUnitAccess {
    private static final Logger LOGGER = LoggerFactory.getLogger(
                    PoolSelectionUnitDecorator.class);

    public enum SelectionAction {
        NONE,
        ADD,
        REMOVE,
        MODIFY
    }

    private PoolSelectionUnitAccess delegate;
    private PoolSelectionUnit psu;
    private CostModule module;

    private volatile boolean active;

    @Override
    public void addLink(String linkName, String name) {
        delegate.addLink(linkName, name);
    }

    @Override
    public void addToLinkGroup(String linkGroupName, String linkName) {
        delegate.addToLinkGroup(linkGroupName, linkName);
    }

    @Override
    public void addToPoolGroup(String pGroupName, String poolName) {
        delegate.addToPoolGroup(pGroupName, poolName);
    }

    @Override
    public void addToUnitGroup(String uGroupName, String unitName,
                    boolean isNet) {
        delegate.addToUnitGroup(uGroupName, unitName, isNet);
    }

    @Override
    public void afterSetup() {
        /*
         *  note that this method is already called by the UniversalSpringCell
         *  on the delegate before the decorator's is invoked.
         */
        active = true;
    }

    @Override
    public void beforeSetup() {
        active = false;
        /*
         *  note that this method is already called by the UniversalSpringCell
         *  on the delegate after the decorator's is invoked.
         */
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public void createLink(String name, ImmutableList<String> unitGroup) {
        delegate.createLink(name, unitGroup);
    }

    @Override
    public void createLinkGroup(String groupName, boolean isReset) {
        delegate.createLinkGroup(groupName, isReset);
    }

    @Override
    public void createPool(String name, boolean isNoPing, boolean isDisabled) {
        delegate.createPool(name, isNoPing, isDisabled);
    }

    @Override
    public void createPoolGroup(String name, boolean isResilient) {
        delegate.createPoolGroup(name, isResilient);
    }

    @Override
    public void createUnit(String name, boolean isNet, boolean isStore,
                    boolean isDcache, boolean isProtocol) {
        delegate.createUnit(name, isNet, isStore, isDcache, isProtocol);
    }

    @Override
    public void createUnitGroup(String name) {
        delegate.createUnitGroup(name);
    }

    @Override
    public String dumpSetup() {
        return delegate.dumpSetup();
    }

    @Override
    public String listLinkGroups(boolean isLongOutput,
                    ImmutableList<String> linkGroups) {
        return delegate.listLinkGroups(isLongOutput, linkGroups);
    }

    @Override
    public Object listLinkXml(boolean isX, boolean resolve, String linkName) {
        return delegate.listLinkXml(isX, resolve, linkName);
    }

    @Override
    public String listNetUnits() {
        return delegate.listNetUnits();
    }

    @Override
    public String listPool(boolean more, boolean detail,
                    ImmutableList<String> globs) {
        return delegate.listPool(more, detail, globs);
    }

    @Override
    public Object listPoolGroupXml(String groupName) {
        return delegate.listPoolGroupXml(groupName);
    }

    @Override
    public String listPoolGroups(boolean more, boolean detail,
                    ImmutableList<String> groups) {
        return delegate.listPoolGroups(more, detail, groups);
    }

    @Override
    public String listPoolLinks(boolean more, boolean detail,
                    ImmutableList<String> links) {
        return delegate.listPoolLinks(more, detail, links);
    }

    @Override
    public Object listPoolXml(String poolName) {
        return delegate.listPoolXml(poolName);
    }

    @Override
    public Object listUnitGroupXml(String groupName) {
        return delegate.listUnitGroupXml(groupName);
    }

    @Override
    public String listUnitGroups(boolean more, boolean detail,
                    ImmutableList<String> unitGroups) {
        return delegate.listUnitGroups(more, detail, unitGroups);
    }

    @Override
    public Object listUnitXml(String poolName) {
        return delegate.listUnitXml(poolName);
    }

    @Override
    public String listUnits(boolean more, boolean detail,
                    ImmutableList<String> units) {
        return delegate.listUnits(more, detail, units);
    }

    @Override
    public String matchLinkGroups(String linkGroup, String op, String storeUnit,
                    String dCacheUnit, String netUnit, String protocolUnit) {
        return delegate.matchLinkGroups(linkGroup, op, storeUnit, dCacheUnit,
                        netUnit, protocolUnit);
    }

    @Override
    public PoolPreferenceLevel[] matchLinkGroupsXml(String linkGroup, String op,
                    String storeUnit, String dCacheUnit, String netUnit,
                    String protocolUnit) {
        return delegate.matchLinkGroupsXml(linkGroup, op, storeUnit, dCacheUnit,
                        netUnit, protocolUnit);
    }

    @Override
    public String matchUnits(String netUnitName, ImmutableList<String> units) {
        return delegate.matchUnits(netUnitName, units);
    }

    @Override
    public String netMatch(String hostAddress) throws UnknownHostException {
        return delegate.netMatch(hostAddress);
    }

    @Override
    public void printSetup(PrintWriter pw) {
        /*
         *  Called by UniversalSpring cell on beans implementing
         *  CellSetupProvider.  Since the delegate also implements that
         *  interface, there is no need to do anything here.
         */
    }

    @Override
    public void removeFromLinkGroup(String linkGroupName, String linkName) {
        delegate.removeFromLinkGroup(linkGroupName, linkName);
    }

    @Override
    public void removeFromPoolGroup(String poolGroupName, String poolName) {
        delegate.removeFromPoolGroup(poolGroupName, poolName);
    }

    @Override
    public void removeFromUnitGroup(String unitGroupName, String unitName,
                    boolean isNet) {
        delegate.removeFromUnitGroup(unitGroupName, unitName, isNet);
    }

    @Override
    public void removeLink(String name) {
        delegate.removeLink(name);
    }

    @Override
    public void removeLinkGroup(String name) {
        delegate.removeLinkGroup(name);
    }

    @Override
    public void removePool(String name) {
        delegate.removePool(name);
    }

    @Override
    public void removePoolGroup(String name) {
        delegate.removePoolGroup(name);
    }

    @Override
    public void removeUnit(String name, boolean isNet) {
        delegate.removeUnit(name, isNet);
    }

    @Override
    public void removeUnitGroup(String name) {
        delegate.removeUnitGroup(name);
    }

    @Override
    public void setAllPoolsActive(String mode) {
        delegate.setAllPoolsActive(mode);
    }

    public void setDelegate(PoolSelectionUnitAccess delegate) {
        this.delegate = delegate;
    }

    @Override
    public void setLink(String linkName, String readPref, String writePref,
                    String cachePref, String p2pPref, String section) {
        delegate.setLink(linkName, readPref, writePref, cachePref, p2pPref,
                        section);
    }

    @Override
    public void setLinkGroup(String linkGroupName, String custodial,
                    String nearline, String online, String output,
                    String replica) {
        delegate.setLinkGroup(linkGroupName, custodial, nearline, online,
                        output, replica);
    }

    public void setCostModule(CostModule module) {
        this.module = module;
    }

    @Override
    public String setPool(String glob, String mode) {
        return delegate.setPool(glob, mode);
    }

    @Override
    public void setPoolActive(String poolName, boolean active) {
        delegate.setPoolActive(poolName, active);
    }

    @Override
    public String setPoolDisabled(String poolName) {
        return delegate.setPoolDisabled(poolName);
    }

    @Override
    public String setPoolEnabled(String poolName) {
        return delegate.setPoolEnabled(poolName);
    }

    public void setPsu(PoolSelectionUnit psu) {
        this.psu = psu;
    }

    @Override
    public String setRegex(String onOff) {
        return delegate.setRegex(onOff);
    }

    /**
     * <p><b>A Note on altering storage units.</b>
     *          How to make a resilient storage-group non-resilient:</p>
     * <ol>
     * <li>If you set minimum to 1, files currently in this unit will have
     *          their replica count reduced to a single copy, and new files in
     *          the unit will not be copied.  But notice that the files do
     *          continue to get scanned as long as the unit is linked to the
     *          resilient pool group.
     * </li>
     * <li>To completely deactivate those files, the unit must be removed
     *          from its unit group.
     * </li>
     * </ol>
     * <p>This call will test to see that all resilient pool groups to which
     *          it is linked can satisfy the requirements for copies
     *          (including partitioning by pool tags).</p>
     */
    @Override
    public void setStorageUnit(String storageUnitKey, Integer required,
                    String[] onlyOneCopyPer) {
        final StorageUnit unit = psu.getStorageUnit(storageUnitKey);

        if (unit == null) {
            throw new IllegalArgumentException("Not found : " + storageUnitKey);
        }

        /*
         * Cannot be done successfully on load, as all links are required
         * to be present for verification purposes.
         */
        if (active) {
            if (required != null) {
                unit.setRequiredCopies(required);
            }

            if (onlyOneCopyPer != null) {
                /*
                 *  We need to anticipate the constraint check done in the
                 *  delegate so as to avoid further validation work.
                 */
                Preconditions.checkArgument(unit.getRequiredCopies() >= 1,
                                "required must be >= 1 in "
                                                + "order to set partition tags, "
                                                + "is currently set to %s.",
                                unit.getRequiredCopies());
                unit.setOnlyOneCopyPer(onlyOneCopyPer);
            }

            StorageUnitInfoExtractor.verifyCanBeSatisfied(unit, psu, module);
        }

        delegate.setStorageUnit(storageUnitKey, required, onlyOneCopyPer);
    }

    @Override
    public void unlink(String linkName, String poolName) {
        delegate.unlink(linkName, poolName);
    }
}
