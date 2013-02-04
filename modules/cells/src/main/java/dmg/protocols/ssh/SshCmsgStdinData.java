package dmg.protocols.ssh ;


public class SshCmsgStdinData extends SshStringPacket {

    public SshCmsgStdinData( SshPacket packet  ){
        super( packet ) ;
    }
    public SshCmsgStdinData(  byte [] string ,  int off , int size ){
        super( null , string , off , size ) ;
    }
}

 
