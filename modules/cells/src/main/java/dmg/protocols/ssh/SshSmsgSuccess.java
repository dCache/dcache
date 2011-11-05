package dmg.protocols.ssh ;
import  dmg.security.cipher.* ;


public class SshSmsgSuccess extends SshPacket {

    public SshSmsgSuccess( StreamCipher cipher ){
        super( cipher ) ;
    }
    public SshSmsgSuccess( ){
        super( null ) ;
    }
    public byte [] toByteArray(){
       return super.makePacket( new byte[0] ) ;
    }
    public byte [] toByteArray(StreamCipher cipher ){
       return super.makePacket( cipher , new byte[0] ) ;
    }
}

