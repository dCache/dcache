/*
 * $Id: NetRolesServerSocketCreator.java,v 1.1 2004-10-06 08:08:27 tigran Exp $
 */

package javatunnel;

import java.net.ServerSocket;
import java.lang.reflect.*;

public class NetRolesServerSocketCreator {


    public  NetRolesServerSocketCreator(String[] args) {

    }




    public ServerSocket createServerSocket( int port ) throws java.io.IOException {
        return new  NetRolesServerSocket(port);
    }

}
