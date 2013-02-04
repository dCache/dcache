/*
 * $Id: NetRolesServerSocketCreator.java,v 1.1 2004-10-06 08:08:27 tigran Exp $
 */

package javatunnel;

import java.io.IOException;
import java.net.ServerSocket;

public class NetRolesServerSocketCreator {


    public  NetRolesServerSocketCreator(String[] args) {

    }




    public ServerSocket createServerSocket( int port ) throws IOException {
        return new  NetRolesServerSocket(port);
    }

}
