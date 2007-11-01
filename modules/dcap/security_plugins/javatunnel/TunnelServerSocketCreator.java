/*
 * $Id: TunnelServerSocketCreator.java,v 1.1 2002-10-14 11:53:07 cvs Exp $
 */

package javatunnel;

import java.net.ServerSocket;
import java.lang.reflect.*;

public class TunnelServerSocketCreator {
    
    
    Convertable tunnel = null;
    
    public TunnelServerSocketCreator(String[] args) {
                
        try {
            Class c  = Class.forName(args[0]);
            Class [] classArgs = { java.lang.String.class } ;
            Constructor cc = c.getConstructor( classArgs );
            Object[] a = new Object[1];
            a[0] = (Object)args[1];
            tunnel = (Convertable)cc.newInstance(a);
        }catch ( Exception e){
        }

    }
    
    
    
    
    public ServerSocket createServerSocket( int port ) throws java.io.IOException {
        return new TunnelServerSocket(port, tunnel);
    }

}
