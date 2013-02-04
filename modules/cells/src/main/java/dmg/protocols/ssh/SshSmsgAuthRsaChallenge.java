package dmg.protocols.ssh ;


public class SshSmsgAuthRsaChallenge extends SshMpIntPacket {

    public SshSmsgAuthRsaChallenge(  
                           byte [] mp , int off , int mpLengthBits ){
      super( mp , off , mpLengthBits ) ;
    }
    public SshSmsgAuthRsaChallenge( SshPacket packet ){ super( packet ) ; } 
}  
