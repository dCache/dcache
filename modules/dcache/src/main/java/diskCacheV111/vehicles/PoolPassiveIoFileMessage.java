/*
 * $Id: PoolPassiveIoFileMessage.java,v 1.3 2007-03-21 08:47:24 tigran Exp $
 */
package diskCacheV111.vehicles;

import java.net.*;

public class PoolPassiveIoFileMessage extends PoolMessage {

	private static final long serialVersionUID = -8019787998659861618L;

	private final InetSocketAddress _socketAddress;
    private final byte[] _challange;

    public PoolPassiveIoFileMessage(String pool, InetSocketAddress socketAddress, byte[] challenge) {
        super(pool);
        _socketAddress = socketAddress;
        _challange = challenge.clone();
    }

    public InetSocketAddress socketAddress() {
        return _socketAddress;
    }

    public byte[] challange() {
        return _challange.clone();
    }

}