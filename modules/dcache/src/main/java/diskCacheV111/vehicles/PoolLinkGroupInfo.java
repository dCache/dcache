/*
 * $Id: PoolLinkGroupInfo.java,v 1.8 2007-10-10 08:05:34 tigran Exp $
 */
package diskCacheV111.vehicles;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLinkGroup;

public class PoolLinkGroupInfo implements Serializable {

    private static final long serialVersionUID = -1670253098493654055L;
    private final String _groupName;
    private final long _totalSpaceInBytes;
    private final long _availableSpaceInBytes;
    private final boolean _custodialAllowed;
    private final boolean _replicaAllowed;
    private final boolean _outputAllowed;
    private final boolean _nearlineAllowed;
    private final boolean _onlineAllowed;
    private final Map<String,Set<String> > _attributes = new HashMap<>();


    public PoolLinkGroupInfo(SelectionLinkGroup linkGroup, long totalSpace, long availableSpace) {
        _groupName = linkGroup.getName();
        _availableSpaceInBytes = availableSpace;
        _totalSpaceInBytes = totalSpace;
        _custodialAllowed = linkGroup.isCustodialAllowed();
        _replicaAllowed = linkGroup.isReplicaAllowed();
        _outputAllowed = linkGroup.isOutputAllowed();
        _nearlineAllowed = linkGroup.isNearlineAllowed();
        _onlineAllowed = linkGroup.isOnlineAllowed();

        Map<String, Set<String>> attributes = linkGroup.attributes();
        if(attributes != null ) {
            _attributes.putAll(attributes);
        }
    }

    /**
     *
     * @return the linkGroup name
     */
    public String getName() {
        return _groupName;
    }

    /**
     *
     * @return total space of all pools in the linkGroup in bytes
     */
    public long getTotalSpace() {
        return _totalSpaceInBytes;
    }

    /**
     *
     * @return available space of all pools in the linkGroup in bytes
     */
    public long getAvailableSpaceInBytes() {
        return _availableSpaceInBytes;
    }

    /**
     *
     * @return true if LinkGroup allows custodial files
     */
    public boolean isCustodialAllowed() {
        return _custodialAllowed;
    }

    /**
     *
     * @return true if LinkGroup allows output files
     */
    public boolean isOutputAllowed() {
        return _outputAllowed;
    }

    /**
     *
     * @return true if LinkGroup allows replica files
     */
    public boolean isReplicaAllowed() {
        return _replicaAllowed;
    }

    /**
     *
     * @return true if LinkGour allows online files
     */
    public boolean isOnlineAllowed() {
        return _onlineAllowed;
    }

    /**
     *
     * @return true if LinkGour allows nearline files
     */
    public boolean isNearlineAllowed() {
        return _nearlineAllowed;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("groupName", _groupName)
                .add("totalSpace", _totalSpaceInBytes)
                .add("availableSpace", _availableSpaceInBytes)
                .add("custodial", _custodialAllowed)
                .add("replica", _replicaAllowed)
                .add("output", _outputAllowed)
                .add("nearline", _nearlineAllowed)
                .add("online", _onlineAllowed)
                .add("attributes", _attributes)
                .toString();
    }
}
/*
 * $Log: not supported by cvs2svn $
 * Revision 1.7  2007/10/03 22:25:36  timur
 * added support for the ling group roles using the syntax lhcb/Role=/lhcb/lhcbprod instead of lhcbRole=/lhcb/lhcbprod old syntax is still supported
 *
 * Revision 1.6  2007/01/12 21:10:22  timur
 * fixed a NullPointerException issue if VOs are not specified in a LinkGroup
 *
 * Revision 1.5  2007/01/09 10:59:07  tigran
 * PoolLinkGroupInfo creates hsmType based on attributes:
 *
 * psu create linkGroup spaceManagerGroup
 * psu set linkGroup attribute spaceManagerGroup -r HSMType=osm
 *
 * Revision 1.4  2007/01/09 10:24:27  tigran
 * PoolLinkGroupInfo created VOInfo based on attributes:
 *
 * psu create linkGroup spaceManagerGroup
 * psu set linkGroup attribute spaceManagerGroup HSMType=osm
 * psu set linkGroup attribute spaceManagerGroup VO=cms
 * psu set linkGroup attribute spaceManagerGroup cmsRole=/cms/NULL/production
 *
 * TODO: this code have to be move into SpaceManager, while PoolManager has no idea about VO .
 *
 * Revision 1.3  2006/12/27 23:03:37  timur
 * take hsm type from the constructor
 *
 * Revision 1.2  2006/10/27 21:32:14  timur
 * changes to support LinkGroups by space manager
 *
 * Revision 1.1  2006/10/10 13:50:49  tigran
 * added linkGroups:
 *
 * i) set of psu commands to manipulate linksGoups
 * ii) PoolManager is able process GetPoolLinkGroups
 *     as a result an array of PoolLinkGroupInfo is returned with
 *     linkGroup names and avaliable space per goup
 * iii) linkGroup may be requested in PoolMgrSelectPoolRequest
 *   if goup is not defined other links is taken
 *
 * TODO:
 *   exclude links whish are member of some groups from regular operations.
 *
 */
