package dmg.protocols.ssh ;

import dmg.security.cipher.StreamCipher;


public class SshCmsgAuthRsa extends SshMpIntPacket {

    public SshCmsgAuthRsa( StreamCipher cipher ,
                           byte [] mp , int off , int mpLengthBits ){
       super( cipher , mp , off , mpLengthBits ) ;
    }
    public SshCmsgAuthRsa( byte [] mp , int off , int mpLengthBits ){
       super( null , mp , off , mpLengthBits ) ;
    }
    public SshCmsgAuthRsa( StreamCipher cipher , byte [] data , int len  ){
      super( cipher , data , len ) ;
    }
    public SshCmsgAuthRsa( byte [] data , int len  ){
      super( null , data , len ) ;
    }
    public SshCmsgAuthRsa( SshPacket packet ){
      super( packet ) ;
    }
    public SshRsaKey getKey(){
        return new SshRsaKey( getMpIntLength() , getMpInt() ) ;
    }
}
