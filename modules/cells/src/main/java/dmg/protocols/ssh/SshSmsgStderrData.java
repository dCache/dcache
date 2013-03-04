package dmg.protocols.ssh ;

import dmg.security.cipher.StreamCipher;


public class SshSmsgStderrData extends SshStringPacket {

    public SshSmsgStderrData( StreamCipher cipher , String string  ){
        super( cipher , string ) ;
    }
    public SshSmsgStderrData( SshPacket packet  ){
        super( packet ) ;
    }
    public SshSmsgStderrData( StreamCipher cipher ,
                              byte [] string ,  int off , int size ){
        super( cipher , string , off , size ) ;
    }
    public SshSmsgStderrData(  byte [] string ,  int off , int size ){
        super( null , string , off , size ) ;
    }
}

