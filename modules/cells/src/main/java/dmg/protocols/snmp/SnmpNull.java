package dmg.protocols.snmp ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpNull extends SnmpObject {

  SnmpNull( SnmpObjectHeader head , byte [] b , int offIn , int maxLen ){
    setCodedLength( head.getCodedLength()  ) ;
  }
  public SnmpNull(){}
  public String toString(){ return "Null" ; }
  @Override
  public byte [] getSnmpBytes(){
     return  
        new SnmpObjectHeader( SnmpObjectHeader.NULL ,0 ).getSnmpBytes() ;
  }
  
}
