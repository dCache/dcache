package dmg.protocols.telnet ;

import  java.net.InetAddress ;

public interface TelnetServerAuthentication {

     public boolean isHostOk( InetAddress host ) ;
     public boolean isUserOk( InetAddress host , String user ) ;
     public boolean isPasswordOk( InetAddress host , String user , String passwd );

}
