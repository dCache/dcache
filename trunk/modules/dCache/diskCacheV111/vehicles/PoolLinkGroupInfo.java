/*
 * $Id: PoolLinkGroupInfo.java,v 1.8 2007-10-10 08:05:34 tigran Exp $
 */
package diskCacheV111.vehicles;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLinkGroup;
import diskCacheV111.util.VOInfo;

public class PoolLinkGroupInfo implements Serializable {

	private final String _groupName;
	private final long _availableSpaceInBytes;
	private final boolean _custodialAllowed;
	private final boolean _replicaAllowed;
	private final boolean _outputAllowed;
	private final boolean _nearlineAllowed;
	private final boolean _onlineAllowed;
	private final Map<String,Set<String> > _attributes = new HashMap<String,Set<String> >(); 
        //
        // I have put some default values for testing purposes 
        // (*,*) will match every vo.
        // 
        private VOInfo[] _allowedVOs = new VOInfo[]{new VOInfo("*","*")};
        //
        // this for retention policy replica and output
        // custodial must have something non-None :)
        //
        private String _hsmType="None";
	
	public PoolLinkGroupInfo(SelectionLinkGroup linkGroup, long availableSpace) {
		_groupName = linkGroup.getName();
		_availableSpaceInBytes = availableSpace;
		_custodialAllowed = linkGroup.isCustodialAllowed();
		_replicaAllowed = linkGroup.isReplicaAllowed();
		_outputAllowed = linkGroup.isOutputAllowed();
		_nearlineAllowed = linkGroup.isNearlineAllowed();
		_onlineAllowed = linkGroup.isOnlineAllowed();
		
		Map<String, Set<String>> attributes = linkGroup.attributes();
		if(attributes != null ) {
			_attributes.putAll(attributes);
			
			
			/*
			 * TODO: this path is GRID/SRM related and has nothing to do with linkGroup
			 */
			
			
			Set<String> suportedVO = attributes.get("VO");
                        if(suportedVO != null) {
                            List <VOInfo> voInfoList = new ArrayList<VOInfo>();
                            for( String vo: suportedVO) {
                                    if(attributes.containsKey(vo+"Role") ) {
                                            Set<String> voRoles = attributes.get(vo+"Role");
                                            for( String voRole: voRoles ) {
                                                    voInfoList.add( new VOInfo(vo, voRole) );
                                            }
                                    }
                                    
                                    if(attributes.containsKey(vo+"/Role") ) {
                                            Set<String> voRoles = attributes.get(vo+"/Role");
                                            for( String voRole: voRoles ) {
                                                    voInfoList.add( new VOInfo(vo, voRole) );
                                            }
                                    }
                                    
                                    if(voInfoList.isEmpty())
                                    {
                                            voInfoList.add( new VOInfo(vo, "*") );
                                    }
                            }

                            _allowedVOs = voInfoList.toArray( new VOInfo[voInfoList.size()]);
                        }
			
			/*
			 * the attribute HSM has to be created with -r option, 
			 * which guarantees to have only one value. In to future, we can add the suport for 
			 * many HSMs
			 * 
			 */
			Set<String> hsmTypeAttribyte =  attributes.get("HSM");
			if( hsmTypeAttribyte != null ) {
				Iterator<String> hsmType  = hsmTypeAttribyte.iterator();
				if(hsmType.hasNext() ) {
					_hsmType = hsmType.next();	
				}
			}			
			
		}
	}        
	
	public String getName() {
		return _groupName;
	}
	
	public long getAvailableSpaceInBytes() {
        return _availableSpaceInBytes;
    }
		
	public Set<String> getAttribute(String attribute) {
		return _attributes.get(attribute);
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
        	
	
	/*
	 * FIXME: this path is GRID/SRM related and has nothing to do with linkGroup
	 */
	
    public VOInfo[] getAllowedVOs() {
        return _allowedVOs;
    }

    public String getHsmType() {
        return _hsmType;
    }

    public void setHsmType(String hsmType) {
        this._hsmType = hsmType;
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