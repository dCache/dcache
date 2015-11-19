package dmg.protocols.telnet ;

import java.net.InetAddress;

public interface TelnetServerAuthentication {

     boolean isHostOk(InetAddress host) ;
     boolean isUserOk(InetAddress host, String user) ;
     boolean isPasswordOk(InetAddress host, String user, String passwd);

}
