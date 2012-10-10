/*
 * $Id: PoolPassiveIoFileMessage.java,v 1.3 2007-03-21 08:47:24 tigran Exp $
 */
package diskCacheV111.vehicles;

import java.io.Serializable;
import java.net.*;

public class PoolPassiveIoFileMessage<T extends Serializable> extends PoolMessage {

    private static final long serialVersionUID = -8019787998659861618L;
    private final InetSocketAddress _socketAddress;
    private final InetSocketAddress[] _socketAddresses;
    private final T _challange;

    public PoolPassiveIoFileMessage(String pool, InetSocketAddress socketAddress, T challenge) {
        this(pool, new InetSocketAddress[] { socketAddress }, challenge);
    }

    public PoolPassiveIoFileMessage(String pool, InetSocketAddress[] socketAddresses, T challenge) {
        super(pool);
        _socketAddresses = socketAddresses;
        _socketAddress = _socketAddresses[0];
        _challange = challenge;
    }

    public InetSocketAddress socketAddress() {
        return _socketAddress;
    }

    public InetSocketAddress[] socketAddresses() {
        /*
         * A bit of crappy code to handle old/new version of the class.
         */
        return _socketAddresses == null?
                new InetSocketAddress[] { _socketAddress } : _socketAddresses;
    }

    public T challange() {
        return _challange;
    }
}