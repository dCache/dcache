package dmg.protocols.ssh ;

import dmg.security.cipher.StreamCipher;


public class SshCmsgUser extends SshStringPacket {

//    public SshCmsgUser( StreamCipher cipher , byte [] data , int len  ){
//        super( cipher , data , len ) ;
//    }
    public SshCmsgUser( SshPacket packet ){  super( packet ) ; }
    public SshCmsgUser( StreamCipher cipher , String str ){
        super( cipher , str ) ;
    }
    public SshCmsgUser( String str ){
        super( null ,str ) ;
    }
    public String getUser(){ return getString() ; }
}

