package dmg.protocols.ssh ;

import dmg.security.cipher.StreamCipher;


public class SshCmsgAuthPassword extends SshStringPacket {

    public SshCmsgAuthPassword( StreamCipher cipher , byte [] data , int len  ){
        super( cipher , data , len ) ;
    }
    public SshCmsgAuthPassword( StreamCipher cipher , String str ){
        super( cipher , str ) ;
    }
    public SshCmsgAuthPassword( String str ){
        super( null , str ) ;
    }
    public SshCmsgAuthPassword( SshPacket packet  ){
        super( packet ) ;
    }
    public String getPassword(){ return getString() ; }
}


