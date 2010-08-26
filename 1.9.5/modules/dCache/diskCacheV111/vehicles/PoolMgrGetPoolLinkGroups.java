/*
 * $Id: PoolMgrGetPoolLinkGroups.java,v 1.2 2006-10-27 21:32:14 timur Exp $
 */
package diskCacheV111.vehicles;

public class PoolMgrGetPoolLinkGroups extends PoolManagerMessage {

	private PoolLinkGroupInfo[] _poolLinkGroupInfos;

	public PoolMgrGetPoolLinkGroups() {
		setReplyRequired(true);
	}

	public String toString() {
		if (getReturnCode() == 0) {
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < getPoolLinkGroupInfos().length; ++i) {
				sb.append('[').append(getPoolLinkGroupInfos()[i]).append(']');
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
