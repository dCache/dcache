package diskCacheV111.poolManager;

import com.google.common.collect.ImmutableList;

import java.net.UnknownHostException;

import dmg.cells.nucleus.CellSetupProvider;

/**
 * Pulled out of the PoolSelectionUnit implementation. An abstraction
 * layer between the command-line and the actual implementation which
 * allows for extension of the behavior.
 *
 * Created by arossi on 2/19/15.
 */
public interface PoolSelectionUnitAccess extends CellSetupProvider {

    void addLink(String linkName, String poolName);

    void addToLinkGroup(String linkGroupName, String linkName);

    void addToPoolGroup(String pGroupName, String poolName);

    void addToUnitGroup(String uGroupName, String unitName, boolean isNet);

    void clear();

    void createLink(String name, ImmutableList<String> unitGroup);

    void createLinkGroup(String groupName, boolean isReset);

    void createPool(String name, boolean isNoPing, boolean isDisabled);

    void createPoolGroup(String name, boolean isResilient);

    void createUnit(String name, boolean isNet, boolean isStore,
                    boolean isDcache, boolean isProtocol);

    void createUnitGroup(String name);

    String dumpSetup();

    String listLinkGroups(boolean isLongOutput,
                    ImmutableList<String> linkGroups);

    Object listLinkXml(boolean isX, boolean resolve, String linkName);

    String listNetUnits();

    String listPool(boolean more, boolean detail, ImmutableList<String> globs);

    String listPoolGroups(boolean more, boolean detail,
                    ImmutableList<String> groups);

    Object listPoolGroupXml(String groupName);

    Object listPoolXml(String poolName);

    String listPoolLinks(boolean more, boolean detail,
                    ImmutableList<String> links);

    String listUnitGroups(boolean more, boolean detail,
                    ImmutableList<String> unitGroups);

    Object listUnitGroupXml(String groupName);

    String listUnits(boolean more, boolean detail, ImmutableList<String> units);

    Object listUnitXml(String poolName);

    String matchUnits(String netUnitName, ImmutableList<String> units);

    String matchLinkGroups(String linkGroup, String op, String storeUnit,
                    String dCacheUnit, String netUnit, String protocolUnit);

    PoolPreferenceLevel[] matchLinkGroupsXml(String linkGroup, String op,
                    String storeUnit, String dCacheUnit, String netUnit,
                    String protocolUnit);

    String netMatch(String hostAddress) throws UnknownHostException;

    void removeFromLinkGroup(String linkGroupName, String linkName);

    void removeFromPoolGroup(String poolGroupName, String poolName);

    void removeFromUnitGroup(String unitGroupName, String unitName, boolean isNet);

    void removeLink(String name);

    void removeLinkGroup(String name);

    void removePool(String name);

    void removePoolGroup(String name);

    void removeUnit(String name, boolean isNet);

    void removeUnitGroup(String name);

    void setAllPoolsActive(String mode);

    String setPool(String glob, String mode);

    void setPoolActive(String poolName, boolean active);

    String setPoolDisabled(String poolName);

    String setPoolEnabled(String poolName);

    void setLink(String linkName, String readPref, String writePref,
                    String cachePref, String p2pPref, String section);

    void setLinkGroup(String linkGroupName, String custodial,
                    String nearline, String online, String output,
                    String replica);

    String setRegex(String onOff);

    void setStorageUnit(String storageUnitKey,
                        Integer required,
                        String[] onlyOneCopyPer);

    void unlink(String linkName, String poolName);
}