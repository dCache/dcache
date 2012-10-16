/*
 * $Id: PoolMgrGetPoolLinkGroups.java,v 1.2 2006-10-27 21:32:14 timur Exp $
 */
package diskCacheV111.vehicles;

public class PoolMgrGetPoolLinkGroups extends PoolManagerMessage {

        private static final long serialVersionUID = -1855943765961354388L;
        private PoolLinkGroupInfo[] _poolLinkGroupInfos;

	public PoolMgrGetPoolLinkGroups() {
		setReplyRequired(true);
	}

	public String toString() {
		if (getReturnCode() == 0 && _poolLinkGroupInfos != null) {
			StringBuilder sb = new StringBuilder();
                    for (PoolLinkGroupInfo info : _poolLinkGroupInfos) {
                        sb.append('[').append(info).append(']');
                    }
			return sb.toString();
		} else {
			return super.toString();
		}
	}

	public PoolLinkGroupInfo[] getPoolLinkGroupInfos() {
		return _poolLinkGroupInfos;
	}

	public void setPoolLinkGroupInfos(PoolLinkGroupInfo[] poolLinkGroupInfos) {
		_poolLinkGroupInfos = poolLinkGroupInfos;
	}

}
