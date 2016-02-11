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
package org.dcache.poolmanager.replication;

import com.google.common.collect.ImmutableList;

import java.io.PrintWriter;
import java.net.UnknownHostException;

import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.poolManager.PoolSelectionUnitAccess;

/**
 * <p>
 * The purpose of this class is to be able to intercept changes from the
 * admin interface and react to them appropriately in terms of the replication
 * system, usually by initiating an update task on a pool or group of
 * pools.
 * </p><p>
 * Note that the interface extends {@link dmg.cells.nucleus.CellSetupProvider}.
 * This is in order to be able to deactivate the post-processing procedures
 * when the poolmanager.conf file is loaded.
 * </p>
 * Created by arossi on 2/20/15.
 */
public final class PoolSelectionUnitDecorator implements PoolSelectionUnitAccess {

    private PoolSelectionUnitAccess delegate;
    private volatile boolean active;

    @Override
    public void addLink(String linkName, String poolName) {
        delegate.addLink(linkName, poolName);
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
        delegate.afterSetup();
        active = true;
    }

    @Override
    public void beforeSetup() {
        active = false;
        delegate.beforeSetup();
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public void  createLink(String name, ImmutableList<String> unitGroup) {
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
    public void createPoolGroup(String name) {
        delegate.createPoolGroup(name);
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
    public String listPoolGroups(boolean more, boolean detail,
                    ImmutableList<String> groups) {
        return delegate.listPoolGroups(more, detail, groups);
    }

    @Override
    public Object listPoolGroupXml(String groupName) {
        return delegate.listPoolGroupXml(groupName);
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
    public String listUnitGroups(boolean more, boolean detail,
                    ImmutableList<String> unitGroups) {
        return delegate.listUnitGroups(more, detail, unitGroups);
    }

    @Override
    public Object listUnitGroupXml(String groupName) {
        return delegate.listUnitGroupXml(groupName);
    }

    @Override
    public String listUnits(boolean more, boolean detail,
                    ImmutableList<String> units) {
        return delegate.listUnits(more, detail, units);
    }

    @Override
    public Object listUnitXml(String poolName) {
        return delegate.listUnitXml(poolName);
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
    public String netMatch(String hostAddress)
                    throws UnknownHostException {
        return delegate.netMatch(hostAddress);
    }

    @Override
    public void printSetup(PrintWriter pw) {
        delegate.printSetup(pw);
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

    @Override
    public String setRegex(String onOff) {
        return delegate.setRegex(onOff);
    }

    @Override
    public void unlink(String linkName, String poolName) {
        delegate.unlink(linkName, poolName);
    }
}
