package dmg.protocols.ssh ;

import dmg.security.cipher.StreamCipher;


public class SshMsgDebug extends SshStringPacket {

    public SshMsgDebug( SshPacket packet ){  super( packet ) ; }
    public SshMsgDebug( StreamCipher cipher , String str ){
        super( cipher , str ) ;
    }
    public String getMessage(){ return getString() ; }
}
