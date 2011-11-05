package dmg.protocols.ssh ;
import  dmg.security.cipher.* ;


public class SshCmsgExecShell extends SshPacket {

    public SshCmsgExecShell( StreamCipher cipher ){
        super( cipher ) ;
    }
    public SshCmsgExecShell( ){
        super( null ) ;
    }
    public byte [] toByteArray(){
       return super.makePacket( new byte[0] ) ;
    }
    public byte [] toByteArray(StreamCipher cipher ){
       return super.makePacket( cipher , new byte[0] ) ;
    }
}
