package diskCacheV111.movers;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Collection;

public class NetIFContainer implements Serializable {

	private static final long serialVersionUID = -859310075936733467L;

	private final String ifName;
	private final Collection<InetAddress> inetAddresses;

	public NetIFContainer(String ifName, Collection<InetAddress> inetAddresses) {
		this.ifName = ifName;
		this.inetAddresses = inetAddresses;
	}

	public String getIfName() {
		return ifName;
	}

	public Collection<InetAddress> getInetAddresses() {
		return inetAddresses;
	}

}
