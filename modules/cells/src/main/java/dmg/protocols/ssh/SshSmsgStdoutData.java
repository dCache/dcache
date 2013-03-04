package dmg.protocols.ssh ;

import dmg.security.cipher.StreamCipher;


public class SshSmsgStdoutData extends SshStringPacket {

    public SshSmsgStdoutData( StreamCipher cipher , String string  ){
        super( cipher , string ) ;
    }
    public SshSmsgStdoutData( SshPacket packet  ){
        super( packet ) ;
    }
    public SshSmsgStdoutData( StreamCipher cipher ,
                              byte [] string ,  int off , int size ){
        super( cipher , string , off , size ) ;
    }
    public SshSmsgStdoutData(  byte [] string ,  int off , int size ){
        super( null , string , off , size ) ;
    }
}


