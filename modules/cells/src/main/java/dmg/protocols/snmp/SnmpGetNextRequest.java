package dmg.protocols.snmp ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpGetNextRequest extends SnmpPDU {
  
  SnmpGetNextRequest( SnmpObjectHeader head , byte [] b , int offIn , int maxLen ){
      super( head , b , offIn , maxLen ) ;
  }
  public SnmpGetNextRequest( SnmpInteger id , SnmpInteger status ,
                  SnmpInteger index , SnmpSequence list   ){
      super( id , status , index ,list ) ;              
                  
  }
  public SnmpGetNextRequest( SnmpSequence snmp ){ super( snmp ) ; }
  public String toString(){ return toString("GetNextRequest") ;}
  
  
} 
