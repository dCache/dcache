package dmg.protocols.ssh ;

import dmg.security.cipher.StreamCipher;


public class SshCmsgExitConfirmation extends SshPacket {

    public SshCmsgExitConfirmation( StreamCipher cipher ){
        super( cipher ) ;
    }
    public SshCmsgExitConfirmation( ){
        super( null ) ;
    }
    @Override
    public byte [] toByteArray(){
       return super.makePacket( new byte[0] ) ;
    }
    @Override
    public byte [] toByteArray(StreamCipher cipher ){
       return super.makePacket( cipher , new byte[0] ) ;
    }
}

