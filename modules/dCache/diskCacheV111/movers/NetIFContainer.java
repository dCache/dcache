package diskCacheV111.movers;

import java.io.Serializable;
import java.util.Collection;

public class NetIFContainer implements Serializable {

	static final long serialVersionUID = -859310075936733467L;

	private final String ifName;
	private final Collection inetAddresses;

	public NetIFContainer(String ifName, Collection inetAddresses) {
		this.ifName = ifName;
		this.inetAddresses = inetAddresses;
	}

	public String getIfName() {
		return ifName;
	}

	public Collection getInetAddresses() {
		return inetAddresses;
	}

}
