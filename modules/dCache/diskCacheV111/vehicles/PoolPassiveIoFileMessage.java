/*
 * $Id: PoolPassiveIoFileMessage.java,v 1.1.2.1 2006-07-17 16:19:31 tigran Exp $
 */
package diskCacheV111.vehicles;

import java.nio.channels.*;
import java.net.*;

public class PoolPassiveIoFileMessage extends PoolMessage {
    
    InetSocketAddress _socketAddress = null;
    byte[] _challange = null;
    
    public PoolPassiveIoFileMessage(String pool, InetSocketAddress socketAddress, byte[] challenge) {
        super(pool);
        _socketAddress = socketAddress;
        _challange = challenge;
    }    
    
    public InetSocketAddress socketAddress() {
        return _socketAddress;
    }
    
    public byte[] challange() {
        return _challange;
    }    
    
}