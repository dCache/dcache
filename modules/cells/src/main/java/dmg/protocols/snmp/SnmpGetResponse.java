package dmg.protocols.snmp ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpGetResponse extends SnmpPDU {
  
  SnmpGetResponse( SnmpObjectHeader head , byte [] b , int offIn , int maxLen ){
      super( head , b , offIn , maxLen ) ;
  }
  public String toString(){ return toString("GetResponse") ;}
  public SnmpGetResponse( SnmpInteger id , SnmpInteger status ,
                  SnmpInteger index , SnmpSequence list   ){
      super( id , status , index ,list ) ;              
                  
  }
  public SnmpGetResponse( SnmpSequence snmp ){ super( snmp ) ; }
  
  
} 
